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
import org.bukkit.event.player.PlayerInteractEvent;
import org.bukkit.event.block.Action;
import org.bukkit.plugin.java.JavaPlugin;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

public final class FlorestRollback extends JavaPlugin implements Listener, CommandExecutor {

    private static FlorestRollback instance;
    private HikariDataSource dataSource;

    // Очередь логов
    private final ConcurrentLinkedQueue<LogEntry> logQueue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger queueSize = new AtomicInteger(0);
    private final AtomicBoolean isFlushing = new AtomicBoolean(false);
    private final Object dbLock = new Object();

    // Для массовых операций WorldEdit
    private final ThreadLocal<List<LogEntry>> batchBuffer = ThreadLocal.withInitial(ArrayList::new);
    private final AtomicInteger worldEditBatchDepth = new AtomicInteger(0);
    private final AtomicInteger worldEditTotalLogged = new AtomicInteger(0);

    // Кэш для инспекторов
    private final Set<UUID> inspectors = Collections.synchronizedSet(new HashSet<>());

    // Статистика
    private final AtomicLong totalLoggedBlocks = new AtomicLong(0);
    private final AtomicLong totalRolledBackBlocks = new AtomicLong(0);

    public static FlorestRollback getInstance() {
        return instance;
    }

    @Override
    public void onEnable() {
        instance = this;
        saveDefaultConfig();
        setupDatabase();

        getServer().getPluginManager().registerEvents(this, this);
        Objects.requireNonNull(getCommand("fr")).setExecutor(this);

        // Регистрация в WorldEdit
        WorldEdit.getInstance().getEventBus().register(this);

        // Периодическая запись в БД
        Bukkit.getAsyncScheduler().runAtFixedRate(this, (task) -> flushQueue(), 1, 5, TimeUnit.SECONDS);

        // Автоочистка старых записей
        Bukkit.getAsyncScheduler().runAtFixedRate(this, (task) -> {
            long days = getConfig().getLong("logging.purge-after-days", 30);
            executePurge(TimeUnit.DAYS.toMillis(days));
        }, 1, 24, TimeUnit.HOURS);

        // Мониторинг очереди
        Bukkit.getAsyncScheduler().runAtFixedRate(this, (task) -> {
            if (queueSize.get() > 4_500_000) {
                getLogger().warning("⚠️ Очередь почти переполнена: " + queueSize.get() + " / 5,000,000");
            }
        }, 1, 1, TimeUnit.MINUTES);

        getLogger().info("✓ FlorestRollback успешно загружен!");
    }

    private void setupDatabase() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:sqlite:" + getDataFolder() + "/" + getConfig().getString("database.file-name", "database.db"));
        config.setMaximumPoolSize(1);
        config.setConnectionTimeout(5000);

        dataSource = new HikariDataSource(config);

        try (Connection conn = dataSource.getConnection();
             Statement st = conn.createStatement()) {

            // Оригинальная структура БД
            st.execute("CREATE TABLE IF NOT EXISTS logs (player TEXT, world TEXT, x INT, y INT, z INT, old_block TEXT, new_block TEXT, time LONG);");
            st.execute("CREATE INDEX IF NOT EXISTS idx_coords ON logs(world, x, z);");
            st.execute("PRAGMA journal_mode=WAL;");
            st.execute("PRAGMA journal_size_limit = 67108864;");
            st.execute("PRAGMA cache_size = -1048576;");
            st.execute("PRAGMA synchronous = OFF;");
            st.execute("PRAGMA temp_store=MEMORY;");
            st.execute("PRAGMA mmap_size=30000000000;");

            getLogger().info("✓ База данных инициализирована");

        } catch (SQLException e) {
            getLogger().log(Level.SEVERE, "❌ Ошибка инициализации БД", e);
        }
    }

    public void addLog(String name, String world, int x, int y, int z, String ob, String nb) {
        // Фильтрация бесполезных записей
        if (ob.equals(nb)) return;
        if (ob.equals("minecraft:air") && nb.equals("minecraft:air")) return;

        // Для массовых операций WorldEdit - буферизация в потоке
        if (worldEditBatchDepth.get() > 0) {
            batchBuffer.get().add(new LogEntry(name, world, x, y, z, ob, nb, System.currentTimeMillis()));
            worldEditTotalLogged.incrementAndGet();
            return;
        }

        // Обычный режим
        if (queueSize.get() < 5000000) {
            logQueue.add(new LogEntry(name, world, x, y, z, ob, nb, System.currentTimeMillis()));
            queueSize.incrementAndGet();
            totalLoggedBlocks.incrementAndGet();
        }
    }

    private void flushQueue() {
        if (logQueue.isEmpty()) return;
        if (!isFlushing.compareAndSet(false, true)) return;

        try {
            synchronized (dbLock) {
                try (Connection conn = dataSource.getConnection()) {
                    conn.setAutoCommit(false);
                    try (PreparedStatement ps = conn.prepareStatement("INSERT INTO logs VALUES (?,?,?,?,?,?,?,?)")) {
                        int batchCount = 0;
                        LogEntry e;

                        while ((e = logQueue.poll()) != null && batchCount < 5000) {
                            queueSize.decrementAndGet();
                            ps.setString(1, e.player);
                            ps.setString(2, e.world);
                            ps.setInt(3, e.x);
                            ps.setInt(4, e.y);
                            ps.setInt(5, e.z);
                            ps.setString(6, e.oldB);
                            ps.setString(7, e.newB);
                            ps.setLong(8, e.time);
                            ps.addBatch();
                            batchCount++;
                        }

                        if (batchCount > 0) {
                            ps.executeBatch();
                            conn.commit();
                        }
                    }
                } catch (SQLException ex) {
                    getLogger().log(Level.SEVERE, "❌ Ошибка записи в БД", ex);
                }
            }
        } finally {
            isFlushing.set(false);
        }
    }

    private void executePurge(long timeMs) {
        long threshold = System.currentTimeMillis() - timeMs;
        Bukkit.getAsyncScheduler().runNow(this, (task) -> {
            synchronized (dbLock) {
                try (Connection conn = dataSource.getConnection()) {
                    try (PreparedStatement ps = conn.prepareStatement("DELETE FROM logs WHERE time < ?")) {
                        ps.setLong(1, threshold);
                        int deleted = ps.executeUpdate();
                        if (deleted > 0) {
                            getLogger().info("🧹 Очистка: удалено " + deleted + " записей.");
                        }
                    }

                    try (Statement st = conn.createStatement()) {
                        st.execute("VACUUM;");
                        getLogger().info("✓ База данных дефрагментирована (VACUUM)");
                    }

                } catch (SQLException e) {
                    getLogger().log(Level.SEVERE, "❌ Ошибка очистки БД", e);
                }
            }
        });
    }

    // ==================== WorldEdit Integration ====================

    @Subscribe
    public void onEditSession(EditSessionEvent event) {
        if (event.getActor() == null) return;

        String playerName = event.getActor().getName();
        String worldName = event.getWorld().getName();

        // Увеличиваем счетчик глубины массовой операции
        worldEditBatchDepth.incrementAndGet();

        // Очищаем буфер для этого потока если он был
        if (batchBuffer.get().isEmpty()) {
            worldEditTotalLogged.set(0);
        }

        event.setExtent(new AbstractDelegateExtent(event.getExtent()) {
            @Override
            public <T extends BlockStateHolder<T>> boolean setBlock(BlockVector3 pos, T block) throws WorldEditException {
                String oldB = getExtent().getBlock(pos).getAsString();
                String newB = block.getAsString();

                if (!oldB.equals(newB)) {
                    addLog(playerName, worldName, pos.x(), pos.y(), pos.z(), oldB, newB);
                }

                return getExtent().setBlock(pos, block);
            }
        });

        // Используем отдельный scheduled task для сброса буфера после завершения EditSession
        // Так как мы не можем переопределить close(), используем хак с задержкой
        Bukkit.getAsyncScheduler().runDelayed(this, (task) -> {
            if (worldEditBatchDepth.decrementAndGet() == 0) {
                List<LogEntry> buffer = batchBuffer.get();
                if (!buffer.isEmpty()) {
                    logQueue.addAll(buffer);
                    queueSize.addAndGet(buffer.size());
                    totalLoggedBlocks.addAndGet(buffer.size());

                    getLogger().info("📦 WorldEdit операция завершена, добавлено " + buffer.size() + " логов в очередь (всего: " + worldEditTotalLogged.get() + ")");

                    buffer.clear();
                    worldEditTotalLogged.set(0);
                }
            }
        }, 1, TimeUnit.MILLISECONDS);
    }

    // ==================== Bukkit Events ====================

    @EventHandler(priority = EventPriority.LOWEST)
    public void onInspect(PlayerInteractEvent e) {
        if (!inspectors.contains(e.getPlayer().getUniqueId())) return;
        if (e.getClickedBlock() == null || e.getAction() != Action.RIGHT_CLICK_BLOCK) return;

        e.setCancelled(true);
        Block b = e.getClickedBlock();
        Player p = e.getPlayer();

        Bukkit.getAsyncScheduler().runNow(this, (task) -> {
            String query = "SELECT player, old_block, new_block, time FROM logs WHERE x=? AND y=? AND z=? AND world=? ORDER BY time DESC LIMIT 5";
            synchronized (dbLock) {
                try (Connection conn = dataSource.getConnection();
                     PreparedStatement ps = conn.prepareStatement(query)) {
                    ps.setInt(1, b.getX());
                    ps.setInt(2, b.getY());
                    ps.setInt(3, b.getZ());
                    ps.setString(4, b.getWorld().getName());

                    ResultSet rs = ps.executeQuery();
                    p.sendMessage("§8--- §6История блока §8---");
                    boolean found = false;
                    while (rs.next()) {
                        found = true;
                        long ago = (System.currentTimeMillis() - rs.getLong("time")) / 1000 / 60;
                        p.sendMessage(String.format("§e%s §7изменил на §f%s §8(%d мин. назад)",
                                rs.getString("player"), rs.getString("new_block"), ago));
                    }
                    if (!found) p.sendMessage("§cИстория пуста.");
                } catch (SQLException ex) {
                    p.sendMessage("§cОшибка при чтении истории");
                    ex.printStackTrace();
                }
            }
        });
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBreak(BlockBreakEvent e) {
        addLog(e.getPlayer().getName(), e.getBlock().getWorld().getName(),
                e.getBlock().getX(), e.getBlock().getY(), e.getBlock().getZ(),
                e.getBlock().getBlockData().getAsString(), "minecraft:air");
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onPlace(BlockPlaceEvent e) {
        addLog(e.getPlayer().getName(), e.getBlock().getWorld().getName(),
                e.getBlock().getX(), e.getBlock().getY(), e.getBlock().getZ(),
                "minecraft:air", e.getBlock().getBlockData().getAsString());
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onExplode(EntityExplodeEvent e) {
        String actor = (e.getEntity() instanceof TNTPrimed tnt && tnt.getSource() instanceof Player p) ? p.getName() : "Explosion";
        for (Block b : e.blockList()) {
            addLog(actor, b.getWorld().getName(), b.getX(), b.getY(), b.getZ(),
                    b.getBlockData().getAsString(), "minecraft:air");
        }
    }

    // ==================== Rollback Logic ====================

    private void handleRollback(Player p, int radius, long timeDelta) {
        flushQueue();

        final World world = p.getWorld();
        final long startTime = System.currentTimeMillis() - timeDelta;
        final int centerCX = p.getLocation().getBlockX() >> 4;
        final int centerCZ = p.getLocation().getBlockZ() >> 4;

        p.sendMessage("§e[FR] Начинаю откат в радиусе " + radius + " чанков...");

        Set<ChunkCoord> chunks = new HashSet<>();
        for (int dx = -radius; dx <= radius; dx++) {
            for (int dz = -radius; dz <= radius; dz++) {
                chunks.add(new ChunkCoord(centerCX + dx, centerCZ + dz));
            }
        }

        final int totalChunks = chunks.size();
        final AtomicInteger processedChunks = new AtomicInteger(0);
        final AtomicInteger totalChanges = new AtomicInteger(0);

        Bukkit.getAsyncScheduler().runNow(this, (task) -> {
            Map<ChunkCoord, List<BlockChange>> changesByChunk = new HashMap<>();

            String query = "SELECT x, y, z, old_block FROM logs " +
                    "WHERE world=? AND time > ? AND x BETWEEN ? AND ? AND z BETWEEN ? AND ? " +
                    "GROUP BY x, y, z ORDER BY time ASC";

            synchronized (dbLock) {
                try (Connection conn = dataSource.getConnection();
                     PreparedStatement ps = conn.prepareStatement(query)) {

                    for (ChunkCoord chunk : chunks) {
                        int minX = chunk.x << 4;
                        int maxX = minX + 15;
                        int minZ = chunk.z << 4;
                        int maxZ = minZ + 15;

                        ps.setString(1, world.getName());
                        ps.setLong(2, startTime);
                        ps.setInt(3, minX);
                        ps.setInt(4, maxX);
                        ps.setInt(5, minZ);
                        ps.setInt(6, maxZ);

                        ResultSet rs = ps.executeQuery();
                        List<BlockChange> changes = new ArrayList<>();

                        while (rs.next()) {
                            changes.add(new BlockChange(
                                    rs.getInt("x"),
                                    rs.getInt("y"),
                                    rs.getInt("z"),
                                    rs.getString("old_block")
                            ));
                        }

                        if (!changes.isEmpty()) {
                            changesByChunk.put(chunk, changes);
                            totalChanges.addAndGet(changes.size());
                        }
                    }

                } catch (SQLException ex) {
                    p.sendMessage("§cОшибка при чтении из БД");
                    ex.printStackTrace();
                    return;
                }
            }

            if (totalChanges.get() == 0) {
                p.sendMessage("§c[FR] Не найдено изменений для отката");
                return;
            }

            p.sendMessage("§e[FR] Найдено " + totalChanges.get() + " изменений. Применяю...");

            for (Map.Entry<ChunkCoord, List<BlockChange>> entry : changesByChunk.entrySet()) {
                ChunkCoord chunk = entry.getKey();
                List<BlockChange> changes = entry.getValue();

                getServer().getRegionScheduler().execute(this, world, chunk.x, chunk.z, () -> {
                    for (BlockChange change : changes) {
                        try {
                            world.setBlockData(change.x, change.y, change.z,
                                    Bukkit.createBlockData(change.oldBlock));
                        } catch (IllegalArgumentException ex) {
                            getLogger().warning("Неверный блок: " + change.oldBlock);
                        }
                    }

                    int processed = processedChunks.incrementAndGet();
                    if (processed % 10 == 0 || processed == totalChunks) {
                        int finalProcessed = processed;
                        Bukkit.getAsyncScheduler().runNow(FlorestRollback.this, (notify) ->
                                p.sendMessage("§7Прогресс: " + finalProcessed + "/" + totalChunks + " чанков")
                        );
                    }

                    if (processed == totalChunks) {
                        totalRolledBackBlocks.addAndGet(totalChanges.get());
                        p.sendMessage("§a[FR] Откат завершен! Изменено блоков: " + totalChanges.get());
                        getLogger().info("Роллбек: " + p.getName() + ", радиус=" + radius +
                                ", время=" + timeDelta/60000 + "мин, блоков=" + totalChanges.get());
                    }
                });
            }
        });
    }

    // ==================== Command Executor ====================

    @Override
    public boolean onCommand(CommandSender sender, Command cmd, String label, String[] args) {
        if (!(sender instanceof Player p)) {
            sender.sendMessage("§cТолько для игроков");
            return true;
        }

        if (args.length == 0) {
            sendHelp(p);
            return true;
        }

        switch (args[0].toLowerCase()) {
            case "rollback":
                int radius = 1;
                long time = TimeUnit.HOURS.toMillis(1);

                for (int i = 1; i < args.length; i++) {
                    if (args[i].startsWith("r:")) {
                        try {
                            radius = Integer.parseInt(args[i].substring(2));
                        } catch (NumberFormatException e) {
                            p.sendMessage("§cНеверный радиус");
                            return true;
                        }
                    } else if (args[i].startsWith("t:")) {
                        time = parseTime(args[i].substring(2));
                    }
                }

                if (radius < 1 || radius > 50) {
                    p.sendMessage("§cРадиус должен быть от 1 до 50");
                    return true;
                }

                handleRollback(p, radius, time);
                return true;

            case "inspect":
            case "i":
                if (inspectors.contains(p.getUniqueId())) {
                    inspectors.remove(p.getUniqueId());
                    p.sendMessage("§c[FR] Режим инспектора выключен.");
                } else {
                    inspectors.add(p.getUniqueId());
                    p.sendMessage("§a[FR] Режим инспектора включен. Кликни по блоку!");
                }
                return true;

            case "purge":
                long t = (args.length > 1) ? parseTime(args[1].replace("t:", "")) : TimeUnit.DAYS.toMillis(30);
                executePurge(t);
                p.sendMessage("§aОчистка запущена в фоновом режиме.");
                return true;

            case "stats":
                p.sendMessage("§6=== Статистика ===");
                p.sendMessage("§7Записано блоков: §e" + totalLoggedBlocks.get());
                p.sendMessage("§7Откачено блоков: §e" + totalRolledBackBlocks.get());
                p.sendMessage("§7Очередь: §e" + queueSize.get() + " / 5,000,000");
                return true;

            default:
                sendHelp(p);
                return true;
        }
    }

    private void sendHelp(Player p) {
        p.sendMessage("§6=== FlorestRollback Help ===");
        p.sendMessage("§e/fr rollback r:<радиус> t:<время> §7- Откатить изменения");
        p.sendMessage("§e  §7Пример: §f/fr rollback r:10 t:1h");
        p.sendMessage("§e/fr inspect §7- Режим просмотра истории блока");
        p.sendMessage("§e/fr purge t:30d §7- Очистить старые записи");
        p.sendMessage("§e/fr stats §7- Показать статистику");
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
    public void onDisable() {
        getLogger().info("Выключение плагина, сброс очереди...");
        flushQueue();

        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }

        getLogger().info("✓ FlorestRollback выключен");
    }

    // ==================== Data Classes ====================

    private record LogEntry(String player, String world, int x, int y, int z, String oldB, String newB, long time) {}

    private record ChunkCoord(int x, int z) {
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ChunkCoord that = (ChunkCoord) o;
            return x == that.x && z == that.z;
        }

        @Override
        public int hashCode() {
            return Objects.hash(x, z);
        }
    }

    private record BlockChange(int x, int y, int z, String oldBlock) {}
}