package ru.florestdev.florestRollback;

import com.sk89q.worldedit.WorldEdit;
import com.sk89q.worldedit.WorldEditException;
import com.sk89q.worldedit.event.extent.EditSessionEvent;
import com.sk89q.worldedit.extent.AbstractDelegateExtent;
import com.sk89q.worldedit.math.BlockVector3;
import com.sk89q.worldedit.util.eventbus.Subscribe;
import com.sk89q.worldedit.world.block.BlockStateHolder;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.bukkit.Bukkit;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import org.bukkit.entity.TNTPrimed;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.block.BlockBreakEvent;
import org.bukkit.event.block.BlockPlaceEvent;
import org.bukkit.event.entity.EntityExplodeEvent;
import org.bukkit.plugin.java.JavaPlugin;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class FlorestRollback extends JavaPlugin implements Listener, CommandExecutor {

    private static FlorestRollback instance;
    private HikariDataSource dataSource;
    private final ConcurrentLinkedQueue<LogEntry> logQueue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger queueSize = new AtomicInteger(0);
    private final AtomicBoolean isFlushing = new AtomicBoolean(false); // Тот самый шлагбаум
    private final Object dbLock = new Object();
    private final Set<UUID> inspectors = Collections.synchronizedSet(new HashSet<>());

    public static FlorestRollback getInstance() { return instance; }

    @Override
    public void onEnable() {
        instance = this;
        saveDefaultConfig();
        setupDatabase();

        getServer().getPluginManager().registerEvents(this, this);
        getCommand("fr").setExecutor(this);
        WorldEdit.getInstance().getEventBus().register(this);

        // Интервал увеличен до 5 сек, чтобы SQLite успевал прожевать пачки
        Bukkit.getAsyncScheduler().runAtFixedRate(this, (task) -> flushQueue(), 1, 5, TimeUnit.SECONDS);

        Bukkit.getAsyncScheduler().runAtFixedRate(this, (task) -> {
            long days = getConfig().getLong("logging.purge-after-days", 30);
            executePurge(TimeUnit.DAYS.toMillis(days));
        }, 1, 24, TimeUnit.HOURS);
    }

    private void setupDatabase() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:sqlite:" + getDataFolder() + "/" + getConfig().getString("database.file-name", "database.db"));
        config.setMaximumPoolSize(1);
        dataSource = new HikariDataSource(config);
        try (Connection conn = dataSource.getConnection()) {
            Statement st = conn.createStatement();
            st.execute("CREATE TABLE IF NOT EXISTS logs (player TEXT, world TEXT, x INT, y INT, z INT, old_block TEXT, new_block TEXT, time LONG);");
            st.execute("CREATE INDEX IF NOT EXISTS idx_coords ON logs(world, x, z);");
            st.execute("PRAGMA journal_mode=WAL;");
            st.execute("PRAGMA journal_size_limit = 67108864;"); // Ограничивает размер лога WAL
            st.execute("PRAGMA cache_size = -1048576;");        // Выделяет 1 ГБ под кэш базы в RAM
            st.execute("PRAGMA synchronous = OFF;");
            st.execute("PRAGMA temp_store=MEMORY;");
            st.execute("PRAGMA mmap_size=30000000000;"); // Выделяем память под маппинг файла (если RAM позволяет)
        } catch (SQLException e) { e.printStackTrace(); }
    }

    public void addLog(String name, String world, int x, int y, int z, String ob, String nb) {
        if (queueSize.get() < 5000000) { // Поднял лимит до 5 млн для тяжелых сфер
            logQueue.add(new LogEntry(name, world, x, y, z, ob, nb, System.currentTimeMillis()));
            queueSize.incrementAndGet();
        }
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void onInspect(org.bukkit.event.player.PlayerInteractEvent e) {
        if (!inspectors.contains(e.getPlayer().getUniqueId())) return;
        if (e.getClickedBlock() == null || e.getAction() != org.bukkit.event.block.Action.RIGHT_CLICK_BLOCK) return;

        e.setCancelled(true); // Чтобы не ставить блоки во время проверки
        Block b = e.getClickedBlock();
        Player p = e.getPlayer();

        Bukkit.getAsyncScheduler().runNow(this, (task) -> {
            String query = "SELECT player, old_block, new_block, time FROM logs WHERE x=? AND y=? AND z=? AND world=? ORDER BY time DESC LIMIT 5";
            synchronized (dbLock) {
                try (Connection conn = dataSource.getConnection();
                     PreparedStatement ps = conn.prepareStatement(query)) {
                    ps.setInt(1, b.getX()); ps.setInt(2, b.getY()); ps.setInt(3, b.getZ());
                    ps.setString(4, b.getWorld().getName());

                    ResultSet rs = ps.executeQuery();
                    p.sendMessage("§8--- §6История блока §8---");
                    boolean found = false;
                    while (rs.next()) {
                        found = true;
                        long ago = (System.currentTimeMillis() - rs.getLong("time")) / 1000 / 60; // минут назад
                        p.sendMessage(String.format("§e%s §7изменил на §f%s §8(%d мин. назад)",
                                rs.getString("player"), rs.getString("new_block"), ago));
                    }
                    if (!found) p.sendMessage("§cИстория пуста.");
                } catch (SQLException ex) { ex.printStackTrace(); }
            }
        });
    }
    @Subscribe
    public void onEditSession(EditSessionEvent event) {
        if (event.getActor() == null) return;
        String playerName = event.getActor().getName();
        String worldName = event.getWorld().getName();

        event.setExtent(new AbstractDelegateExtent(event.getExtent()) {
            @Override
            public <T extends BlockStateHolder<T>> boolean setBlock(BlockVector3 pos, T block) throws WorldEditException {
                // Получаем состояние ДО и ПОСЛЕ
                String oldB = getExtent().getBlock(pos).getAsString();
                String newB = block.getAsString();

                // ОПТИМИЗАЦИЯ: Если блок не меняется (например, сетим камень на камень),
                // или это замена воздуха на воздух — игнорируем, чтобы не грузить базу.
                if (!oldB.equals(newB)) {
                    addLog(playerName, worldName, pos.x(), pos.y(), pos.z(), oldB, newB);
                }

                return getExtent().setBlock(pos, block);
            }
        });
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBreak(BlockBreakEvent e) {
        addLog(e.getPlayer().getName(), e.getBlock().getWorld().getName(), e.getBlock().getX(), e.getBlock().getY(), e.getBlock().getZ(), e.getBlock().getBlockData().getAsString(), "minecraft:air");
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onPlace(BlockPlaceEvent e) {
        addLog(e.getPlayer().getName(), e.getBlock().getWorld().getName(), e.getBlock().getX(), e.getBlock().getY(), e.getBlock().getZ(), "minecraft:air", e.getBlock().getBlockData().getAsString());
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onExplode(EntityExplodeEvent e) {
        String actor = (e.getEntity() instanceof TNTPrimed tnt && tnt.getSource() instanceof Player p) ? p.getName() : "Explosion";
        for (Block b : e.blockList()) {
            addLog(actor, b.getWorld().getName(), b.getX(), b.getY(), b.getZ(), b.getBlockData().getAsString(), "minecraft:air");
        }
    }

    @Override
    public boolean onCommand(CommandSender sender, Command cmd, String label, String[] args) {
        if (!(sender instanceof Player p)) return true;
        if (args.length > 0 && args[0].equalsIgnoreCase("rollback")) {
            int radius = 1; long time = TimeUnit.HOURS.toMillis(1);
            for (String arg : args) {
                if (arg.startsWith("r:")) radius = Integer.parseInt(arg.substring(2));
                if (arg.startsWith("t:")) time = parseTime(arg.substring(2));
            }
            handleRollback(p, radius, time);
            return true;
        }

        if (args.length > 0 && args[0].equalsIgnoreCase("i")) {
            if (inspectors.contains(p.getUniqueId())) {
                inspectors.remove(p.getUniqueId());
                p.sendMessage("§c[FR] Режим инспектора выключен.");
            } else {
                inspectors.add(p.getUniqueId());
                p.sendMessage("§a[FR] Режим инспектора включен. Кликни по блоку!");
            }
            return true;
        }

        if (args.length > 0 && args[0].equalsIgnoreCase("purge")) {
            long t = (args.length > 1) ? parseTime(args[1].replace("t:", "")) : TimeUnit.DAYS.toMillis(30);
            executePurge(t);
            p.sendMessage("§aОчистка запущена.");
            return true;
        }
        return false;
    }

    private void handleRollback(Player p, int radius, long timeDelta) {
        flushQueue();
        final World world = p.getWorld();
        final long startTime = System.currentTimeMillis() - timeDelta;
        final int centerCX = p.getLocation().getBlockX() >> 4;
        final int centerCZ = p.getLocation().getBlockZ() >> 4;

        Bukkit.getAsyncScheduler().runNow(this, (task) -> {
            final AtomicInteger count = new AtomicInteger(0);
            int minX = (centerCX - radius) << 4; int maxX = ((centerCX + radius) << 4) + 15;
            int minZ = (centerCZ - radius) << 4; int maxZ = ((centerCZ + radius) << 4) + 15;

            String query = "SELECT x, y, z, old_block FROM logs " +
                    "WHERE world=? AND time > ? AND x BETWEEN ? AND ? AND z BETWEEN ? AND ? " +
                    "GROUP BY x, y, z ORDER BY time ASC";

            synchronized (dbLock) {
                try (Connection conn = dataSource.getConnection();
                     PreparedStatement ps = conn.prepareStatement(query)) {
                    ps.setString(1, world.getName()); ps.setLong(2, startTime);
                    ps.setInt(3, minX); ps.setInt(4, maxX);
                    ps.setInt(5, minZ); ps.setInt(6, maxZ);

                    ResultSet rs = ps.executeQuery();
                    while (rs.next()) {
                        final int x = rs.getInt("x"); final int y = rs.getInt("y"); final int z = rs.getInt("z");
                        final String data = rs.getString("old_block");
                        Bukkit.getRegionScheduler().execute(this, world, x >> 4, z >> 4, () -> {
                            world.setBlockData(x, y, z, Bukkit.createBlockData(data));
                            count.incrementAndGet();
                        });
                    }
                    p.sendMessage("§a[FR] Откат завершен. Изменено: " + count.get());
                } catch (SQLException ex) { ex.printStackTrace(); }
            }
        });
    }

    private void flushQueue() {
        if (logQueue.isEmpty()) return;

        // ЗАЩИТА: Если уже пишем - уходим, чтобы не плодить транзакции
        if (!isFlushing.compareAndSet(false, true)) return;

        try {
            synchronized (dbLock) {
                try (Connection conn = dataSource.getConnection()) {
                    conn.setAutoCommit(false);
                    try (PreparedStatement ps = conn.prepareStatement("INSERT INTO logs VALUES (?,?,?,?,?,?,?,?)")) {
                        LogEntry e;
                        while ((e = logQueue.poll()) != null) {
                            queueSize.decrementAndGet();
                            ps.setString(1, e.player); ps.setString(2, e.world);
                            ps.setInt(3, e.x); ps.setInt(4, e.y); ps.setInt(5, e.z);
                            ps.setString(6, e.oldB); ps.setString(7, e.newB);
                            ps.setLong(8, e.time);
                            ps.addBatch();
                        }
                        ps.executeBatch();
                        conn.commit();
                    }
                } catch (SQLException ex) { ex.printStackTrace(); }
            }
        } finally {
            isFlushing.set(false); // Открываем шлагбаум
        }
    }

    private void executePurge(long timeMs) {
        long threshold = System.currentTimeMillis() - timeMs;
        Bukkit.getAsyncScheduler().runNow(this, (task) -> {
            synchronized (dbLock) {
                try (Connection conn = dataSource.getConnection()) {
                    // 1. Сначала удаляем старые записи из таблицы
                    try (PreparedStatement ps = conn.prepareStatement("DELETE FROM logs WHERE time < ?")) {
                        ps.setLong(1, threshold);
                        int deleted = ps.executeUpdate();
                        // Можно вывести в консоль, сколько строк почистили
                        if (deleted > 0) {
                            getLogger().info("[FR] Очистка: удалено " + deleted + " записей.");
                        }
                    }

                    // 2. Выполняем сжатие файла (освобождаем место на SSD)
                    // Это может занять время, если база огромная, но благодаря AsyncScheduler сервер не зависнет.
                    try (Statement st = conn.createStatement()) {
                        st.execute("VACUUM;");
                        getLogger().info("[FR] База данных дефрагментирована (VACUUM завершен).");
                    }

                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private long parseTime(String input) {
        try {
            long num = Long.parseLong(input.replaceAll("[^0-9]", ""));
            if (input.endsWith("m")) return TimeUnit.MINUTES.toMillis(num);
            if (input.endsWith("h")) return TimeUnit.HOURS.toMillis(num);
            if (input.endsWith("d")) return TimeUnit.DAYS.toMillis(num);
        } catch (Exception ignored) {}
        return TimeUnit.HOURS.toMillis(1);
    }

    @Override
    public void onDisable() { flushQueue(); if (dataSource != null) dataSource.close(); }

    private record LogEntry(String player, String world, int x, int y, int z, String oldB, String newB, long time) {}
}