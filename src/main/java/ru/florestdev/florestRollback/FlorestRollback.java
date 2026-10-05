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
import org.bukkit.*;
import org.bukkit.block.Block;
import org.bukkit.block.BlockState;
import org.bukkit.block.Container;
import org.bukkit.block.data.BlockData;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.command.PluginCommand;
import org.bukkit.command.TabCompleter;
import org.bukkit.entity.*;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.block.*;
import org.bukkit.event.entity.EntityDeathEvent;
import org.bukkit.event.entity.EntityExplodeEvent;
import org.bukkit.event.entity.EntityPickupItemEvent;
import org.bukkit.event.hanging.HangingBreakEvent;
import org.bukkit.event.hanging.HangingPlaceEvent;
import org.bukkit.event.inventory.*;
import org.bukkit.event.player.*;
import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.ItemStack;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.util.io.BukkitObjectInputStream;
import org.bukkit.util.io.BukkitObjectOutputStream;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

public final class FlorestRollback extends JavaPlugin implements Listener, CommandExecutor, TabCompleter {

    private static FlorestRollback instance;

    private HikariDataSource dataSource;

    // ==================== ОЧЕРЕДЬ ====================
    private final ConcurrentLinkedQueue<LogEntry> logQueue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger queueSize = new AtomicInteger(0);
    private final AtomicBoolean isFlushing = new AtomicBoolean(false);
    private final Object dbLock = new Object();

    private static final int MAX_QUEUE = 2_000_000;
    private static final int BATCH_SIZE = 5_000;

    // WAL
    private final Object walLock = new Object();
    private Path walPath;
    private BufferedWriter walWriter;

    // WorldEdit
    private final ThreadLocal<List<LogEntry>> batchBuffer = ThreadLocal.withInitial(ArrayList::new);
    private final AtomicInteger worldEditBatchDepth = new AtomicInteger(0);
    private final AtomicInteger worldEditTotalLogged = new AtomicInteger(0);

    // Инспекторы
    private final Set<UUID> inspectors = Collections.synchronizedSet(new HashSet<>());

    // Статистика
    private final AtomicLong totalLoggedBlocks = new AtomicLong(0);
    private final AtomicLong totalRolledBackBlocks = new AtomicLong(0);

    // Кэш BlockData
    private final Map<String, BlockData> blockDataCache = new ConcurrentHashMap<>();

    // Открытые контейнеры
    private final Map<UUID, ContainerSession> openContainers = new ConcurrentHashMap<>();

    public static FlorestRollback getInstance() {
        return instance;
    }

    // ==================== ЖИЗНЕННЫЙ ЦИКЛ ====================
    @Override
    public void onEnable() {
        instance = this;
        saveDefaultConfig();
        setupDatabase();
        setupWal();
        getServer().getPluginManager().registerEvents(this, this);

        PluginCommand frCommand = Objects.requireNonNull(getCommand("fr"));
        frCommand.setExecutor(this);
        frCommand.setTabCompleter(this);

        try {
            WorldEdit.getInstance().getEventBus().register(this);
        } catch (Throwable ignored) {
            getLogger().warning("WorldEdit не найден — интеграция отключена");
        }

        Bukkit.getAsyncScheduler().runAtFixedRate(this, (task) -> flushQueueSafe(),
                1, 2, TimeUnit.SECONDS);

        Bukkit.getAsyncScheduler().runAtFixedRate(this, (task) -> {
            long days = getConfig().getLong("logging.purge-after-days", 30);
            executePurge(TimeUnit.DAYS.toMillis(days));
        }, 1, 24, TimeUnit.HOURS);

        Bukkit.getAsyncScheduler().runAtFixedRate(this, (task) -> {
            if (queueSize.get() > MAX_QUEUE * 0.9) {
                getLogger().warning("⚠️ Очередь почти переполнена: " + queueSize.get() + " / " + MAX_QUEUE);
            }
        }, 1, 1, TimeUnit.MINUTES);

        getLogger().info("✓ FlorestRollback успешно загружен!");
    }

    @Override
    public void onDisable() {
        getLogger().info("Выключение плагина, финальный сброс очереди...");
        flushQueueSafe();
        closeWal();
        if (dataSource != null && !dataSource.isClosed()) dataSource.close();
        getLogger().info("✓ FlorestRollback выключен");
    }

    // ==================== БД ====================
    private void setupDatabase() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:sqlite:" + getDataFolder() + "/" +
                getConfig().getString("database.file-name", "database.db"));
        config.setMaximumPoolSize(1);
        config.setConnectionTimeout(5000);
        dataSource = new HikariDataSource(config);

        try (Connection conn = dataSource.getConnection();
             Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS logs (" +
                    "player TEXT, world TEXT, x INT, y INT, z INT, " +
                    "old_block TEXT, new_block TEXT, time LONG);");
            st.execute("CREATE INDEX IF NOT EXISTS idx_world_time ON logs(world, time);");
            st.execute("CREATE INDEX IF NOT EXISTS idx_world_coords_time ON logs(world, x, z, time);");
            st.execute("CREATE INDEX IF NOT EXISTS idx_coords ON logs(x, y, z, world);");
            st.execute("CREATE INDEX IF NOT EXISTS idx_time ON logs(time);");
            st.execute("PRAGMA journal_mode=WAL;");
            st.execute("PRAGMA journal_size_limit = 67108864;");
            st.execute("PRAGMA cache_size = -524288;");
            st.execute("PRAGMA synchronous = OFF;");
            st.execute("PRAGMA temp_store=MEMORY;");
            st.execute("PRAGMA mmap_size=30000000000;");
            st.execute("PRAGMA page_size=4096;");
            getLogger().info("✓ База данных инициализирована");
        } catch (SQLException e) {
            getLogger().log(Level.SEVERE, "❌ Ошибка инициализации БД", e);
        }
    }

    // ==================== WAL ====================
    private void setupWal() {
        try {
            Files.createDirectories(getDataFolder().toPath());
            walPath = getDataFolder().toPath().resolve("wal.log");
            walWriter = new BufferedWriter(new FileWriter(walPath.toFile(), true));
            restoreFromWal();
        } catch (IOException e) {
            getLogger().log(Level.SEVERE, "❌ Не удалось инициализировать WAL", e);
        }
    }

    private void restoreFromWal() {
        if (!Files.exists(walPath)) return;
        try {
            List<String> lines = Files.readAllLines(walPath, StandardCharsets.UTF_8);
            if (lines.isEmpty()) return;
            int restored = 0;
            for (String line : lines) {
                LogEntry e = LogEntry.fromWal(line);
                if (e != null) {
                    logQueue.add(e);
                    queueSize.incrementAndGet();
                    restored++;
                }
            }
            if (restored > 0) {
                getLogger().info("📼 Восстановлено из WAL: " + restored + " записей");
            }
            Files.write(walPath, new byte[0]);
        } catch (IOException e) {
            getLogger().log(Level.SEVERE, "❌ Ошибка восстановления из WAL", e);
        }
    }

    private void appendToWal(LogEntry e) {
        synchronized (walLock) {
            try {
                walWriter.write(e.toWal());
                walWriter.write('\n');
                walWriter.flush();
            } catch (IOException ex) {
                getLogger().log(Level.SEVERE, "❌ Ошибка записи в WAL", ex);
            }
        }
    }

    /**
     * Перезаписывает WAL только актуальными (ещё не сброшенными) записями.
     * Вызывается после успешного коммита в БД.
     */
    private void rewriteWal() {
        synchronized (walLock) {
            try {
                if (walWriter != null) walWriter.close();
                List<LogEntry> remaining = new ArrayList<>(logQueue);
                try (BufferedWriter w = new BufferedWriter(new FileWriter(walPath.toFile(), false))) {
                    for (LogEntry e : remaining) {
                        w.write(e.toWal());
                        w.write('\n');
                    }
                    w.flush();
                }
                walWriter = new BufferedWriter(new FileWriter(walPath.toFile(), true));
            } catch (IOException ex) {
                getLogger().log(Level.SEVERE, "❌ Ошибка перезаписи WAL", ex);
            }
        }
    }

    private void closeWal() {
        synchronized (walLock) {
            try {
                if (walWriter != null) walWriter.close();
            } catch (IOException ignored) {}
        }
    }

    // ==================== ПУБЛИЧНЫЙ API ====================
    public void addLog(String name, String world, int x, int y, int z, String ob, String nb) {
        if (ob == null || nb == null) return;
        if (ob.equals(nb)) return;

        LogEntry entry = new LogEntry(name, world, x, y, z, ob, nb, System.currentTimeMillis());

        if (worldEditBatchDepth.get() > 0) {
            batchBuffer.get().add(entry);
            worldEditTotalLogged.incrementAndGet();
            return;
        }

        enqueue(entry);
    }

    private void enqueue(LogEntry entry) {
        appendToWal(entry);

        if (queueSize.get() >= MAX_QUEUE) {
            getLogger().warning("⚠️ Переполнение очереди, принудительный сброс...");
            flushQueueSafe();

            int attempts = 0;
            while (queueSize.get() >= MAX_QUEUE && attempts < 200) {
                try { Thread.sleep(50); } catch (InterruptedException ignored) {}
                attempts++;
            }

            if (queueSize.get() >= MAX_QUEUE) {
                getLogger().severe("❌ Не удалось сбросить очередь! Лог сохранён только в WAL: " + entry);
                return;
            }
        }

        logQueue.add(entry);
        queueSize.incrementAndGet();
        totalLoggedBlocks.incrementAndGet();
    }

    public boolean logBlockBreak(String playerName, World world, Block block) {
        addLog(playerName, world.getName(),
                block.getX(), block.getY(), block.getZ(),
                serializeBlock(block), "minecraft:air");
        return true;
    }

    public boolean logBlockPlace(String playerName, World world, Block block) {
        addLog(playerName, world.getName(),
                block.getX(), block.getY(), block.getZ(),
                "minecraft:air", serializeBlock(block));
        return true;
    }

    public boolean logBlockReplace(String playerName, World world, Block block, String oldBlock, String newBlock) {
        addLog(playerName, world.getName(),
                block.getX(), block.getY(), block.getZ(),
                oldBlock, newBlock);
        return true;
    }

    public void forceFlush() {
        flushQueueSafe();
        int attempts = 0;
        while (isFlushing.get() && attempts < 100) {
            try { Thread.sleep(10); attempts++; } catch (InterruptedException ignored) {}
        }
    }

    public int getQueueSize() { return queueSize.get(); }
    public long getTotalLoggedBlocks() { return totalLoggedBlocks.get(); }
    public long getTotalRolledBackBlocks() { return totalRolledBackBlocks.get(); }

    // ==================== СЕРИАЛИЗАЦИЯ ====================
    private String serializeBlock(Block block) {
        BlockData data = block.getBlockData();
        String base = data.getAsString();
        BlockState state = block.getState();

        if (state instanceof Container container) {
            try {
                ItemStack[] contents = container.getInventory().getContents();
                String nbt = serializeInventory(contents);
                return "NBT|" + base + "|" + nbt;
            } catch (Throwable t) {
                return base;
            }
        }
        return base;
    }

    @SuppressWarnings("deprecation")
    private void deserializeAndPlace(World world, int x, int y, int z, String raw) {
        try {
            if (raw == null || raw.isEmpty()) return;

            if (raw.startsWith("NBT|")) {
                String[] parts = raw.split("\\|", 3);
                if (parts.length < 3) {
                    applyBlockData(world, x, y, z, parts.length >= 2 ? parts[1] : "minecraft:air");
                    return;
                }
                BlockData data = safeBlockData(parts[1]);
                // Сначала ставим блок, потом восстанавливаем NBT
                BlockState state1 = world.getBlockAt(x, y, z).getState();
                state1.setBlockData(data);
                state1.update(true, false); // force=true 强制覆盖，applyPhysics=false 试图抑制物理

                Block block = world.getBlockAt(x, y, z);
                BlockState state = block.getState();
                if (state instanceof Container container) {
                    ItemStack[] contents = deserializeInventory(parts[2]);
                    container.getInventory().setContents(contents);
                    container.update(true, false);
                }
            } else {
                applyBlockData(world, x, y, z, raw);
            }
        } catch (Exception ex) {
            getLogger().warning("Ошибка при установке блока " + raw + ": " + ex.getMessage());
        }
    }

    private void applyBlockData(World world, int x, int y, int z, String raw) {
        BlockData data = safeBlockData(raw);
        BlockState state = world.getBlockAt(x, y, z).getState();
        state.setBlockData(data);
        state.update(true, false);
    }

    private BlockData safeBlockData(String raw) {
        return blockDataCache.computeIfAbsent(raw, k -> {
            try {
                return Bukkit.createBlockData(k);
            } catch (IllegalArgumentException ex) {
                try {
                    return Bukkit.createBlockData("minecraft:" + k);
                } catch (IllegalArgumentException ex2) {
                    getLogger().warning("Неизвестный BlockData: " + k + " — использую air");
                    return Bukkit.createBlockData(Material.AIR);
                }
            }
        });
    }

    private String serializeInventory(ItemStack[] contents) {
        try {
            try {
                java.lang.reflect.Method m = ItemStack.class.getMethod("serializeAsBytes");
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos);
                dos.writeInt(contents.length);
                for (ItemStack item : contents) {
                    if (item == null) {
                        dos.writeInt(0);
                    } else {
                        byte[] bytes = (byte[]) m.invoke(item);
                        dos.writeInt(bytes.length);
                        dos.write(bytes);
                    }
                }
                dos.flush();
                return Base64.getEncoder().encodeToString(baos.toByteArray());
            } catch (NoSuchMethodException nsme) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (BukkitObjectOutputStream boos = new BukkitObjectOutputStream(baos)) {
                    boos.writeInt(contents.length);
                    for (ItemStack item : contents) boos.writeObject(item);
                }
                return Base64.getEncoder().encodeToString(baos.toByteArray());
            }
        } catch (Throwable t) {
            getLogger().log(Level.WARNING, "Не удалось сериализовать инвентарь", t);
            return "";
        }
    }

    private ItemStack[] deserializeInventory(String base64) {
        if (base64 == null || base64.isEmpty()) return new ItemStack[0];
        try {
            byte[] raw = Base64.getDecoder().decode(base64);
            try {
                Class<?> clazz = Class.forName("org.bukkit.inventory.ItemStack");
                java.lang.reflect.Method m = clazz.getMethod("deserializeBytes", byte[].class);
                DataInputStream dis = new DataInputStream(new ByteArrayInputStream(raw));
                int len = dis.readInt();
                ItemStack[] out = new ItemStack[len];
                for (int i = 0; i < len; i++) {
                    int blen = dis.readInt();
                    if (blen == 0) { out[i] = null; continue; }
                    byte[] bytes = new byte[blen];
                    dis.readFully(bytes);
                    out[i] = (ItemStack) m.invoke(null, (Object) bytes);
                }
                return out;
            } catch (NoSuchMethodException nsme) {
                try (BukkitObjectInputStream bois = new BukkitObjectInputStream(new ByteArrayInputStream(raw))) {
                    int len = bois.readInt();
                    ItemStack[] out = new ItemStack[len];
                    for (int i = 0; i < len; i++) out[i] = (ItemStack) bois.readObject();
                    return out;
                }
            }
        } catch (Throwable t) {
            getLogger().log(Level.WARNING, "Не удалось десериализовать инвентарь", t);
            return new ItemStack[0];
        }
    }

    // ==================== FLUSH ====================
    private void flushQueueSafe() {
        try {
            flushQueue();
        } catch (Throwable t) {
            getLogger().log(Level.SEVERE, "Ошибка в flushQueue", t);
        }
    }

    private void flushQueue() {
        if (logQueue.isEmpty()) return;
        if (!isFlushing.compareAndSet(false, true)) return;

        try {
            synchronized (dbLock) {
                try (Connection conn = dataSource.getConnection()) {
                    conn.setAutoCommit(false);
                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO logs VALUES (?,?,?,?,?,?,?,?)")) {
                        int batchCount = 0;
                        LogEntry e;
                        while ((e = logQueue.poll()) != null && batchCount < BATCH_SIZE) {
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
                    getLogger().log(Level.SEVERE, "❌ Ошибка записи в БД (данные в WAL сохранены)", ex);
                }
            }
            // После коммита перезаписываем WAL только оставшимися записями
            rewriteWal();
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
                        if (deleted > 0) getLogger().info("🧹 Очистка: удалено " + deleted + " записей.");
                    }
                    try (Statement st = conn.createStatement()) {
                        st.execute("VACUUM;");
                    }
                } catch (SQLException e) {
                    getLogger().log(Level.SEVERE, "❌ Ошибка очистки БД", e);
                }
            }
        });
    }

    // ==================== WorldEdit ====================
    @Subscribe
    public void onEditSession(EditSessionEvent event) {
        if (event.getActor() == null) return;
        String playerName = event.getActor().getName();
        String worldName = event.getWorld().getName();

        worldEditBatchDepth.incrementAndGet();

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

        Bukkit.getAsyncScheduler().runDelayed(this, (task) -> {
            if (worldEditBatchDepth.decrementAndGet() == 0) {
                List<LogEntry> buffer = batchBuffer.get();
                if (!buffer.isEmpty()) {
                    for (LogEntry e : buffer) enqueue(e);
                    getLogger().info("📦 WorldEdit операция завершена, добавлено " + buffer.size() +
                            " логов (всего: " + worldEditTotalLogged.get() + ")");
                    buffer.clear();
                    worldEditTotalLogged.set(0);
                }
            }
        }, 1, TimeUnit.MILLISECONDS);
    }

    // ==================== INSPECTOR ====================
    @EventHandler(priority = EventPriority.LOWEST)
    public void onInspect(PlayerInteractEvent e) {
        if (!inspectors.contains(e.getPlayer().getUniqueId())) return;
        if (e.getClickedBlock() == null || e.getAction() != Action.RIGHT_CLICK_BLOCK) return;
        if (e.getPlayer().isSneaking()) return;

        e.setCancelled(true);
        Block b = e.getClickedBlock();
        Player p = e.getPlayer();

        Bukkit.getAsyncScheduler().runNow(this, (task) -> {
            String query = "SELECT player, old_block, new_block, time FROM logs " +
                    "WHERE x=? AND y=? AND z=? AND world=? ORDER BY time DESC LIMIT 10";
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
                        p.sendMessage(String.format("§e%s §7изменил §f%s §8(%d мин. назад)",
                                rs.getString("player"),
                                stripNbt(rs.getString("new_block")), ago));
                    }
                    if (!found) p.sendMessage("§cИстория пуста.");
                } catch (SQLException ex) {
                    p.sendMessage("§cОшибка при чтении истории");
                    ex.printStackTrace();
                }
            }
        });
    }

    private String stripNbt(String s) {
        if (s == null) return "?";
        if (s.startsWith("NBT|")) {
            String[] parts = s.split("\\|", 3);
            return parts.length >= 2 ? parts[1] + " §7[NBT]" : "container";
        }
        return s;
    }

    // ==================== БАЗОВЫЕ ИВЕНТЫ ====================
    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBreak(BlockBreakEvent e) {
        addLog(e.getPlayer().getName(), e.getBlock().getWorld().getName(),
                e.getBlock().getX(), e.getBlock().getY(), e.getBlock().getZ(),
                serializeBlock(e.getBlock()), "minecraft:air");
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onPlace(BlockPlaceEvent e) {
        addLog(e.getPlayer().getName(), e.getBlock().getWorld().getName(),
                e.getBlock().getX(), e.getBlock().getY(), e.getBlock().getZ(),
                "minecraft:air", serializeBlock(e.getBlock()));
    }

    // ==================== ВЗРЫВЫ ====================
    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onExplode(EntityExplodeEvent e) {
        String actor = (e.getEntity() instanceof TNTPrimed tnt && tnt.getSource() instanceof Player p)
                ? p.getName()
                : "Explosion:" + e.getEntityType().name();
        for (Block b : e.blockList()) {
            addLog(actor, b.getWorld().getName(), b.getX(), b.getY(), b.getZ(),
                    serializeBlock(b), "minecraft:air");
        }
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockExplode(BlockExplodeEvent e) {
        for (Block b : e.blockList()) {
            addLog("BlockExplode:" + e.getBlock().getType().name(),
                    b.getWorld().getName(), b.getX(), b.getY(), b.getZ(),
                    serializeBlock(b), "minecraft:air");
        }
    }

    // ==================== КОНТЕЙНЕРЫ ====================
    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onInventoryOpen(InventoryOpenEvent e) {
        if (!(e.getPlayer() instanceof Player p)) return;
        if (!(e.getInventory().getHolder() instanceof Container container)) return;
        Block b = container.getBlock();
        openContainers.put(p.getUniqueId(), new ContainerSession(
                b.getWorld().getName(), b.getX(), b.getY(), b.getZ(),
                serializeBlock(b), p.getName()
        ));
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onInventoryClose(InventoryCloseEvent e) {
        if (!(e.getPlayer() instanceof Player p)) return;
        ContainerSession session = openContainers.remove(p.getUniqueId());
        if (session == null) return;

        World world = Bukkit.getWorld(session.world);
        if (world == null) return;
        Block b = world.getBlockAt(session.x, session.y, session.z);
        if (!(b.getState() instanceof Container)) return;

        String current = serializeBlock(b);
        if (!current.equals(session.snapshot)) {
            addLog(p.getName(), session.world, session.x, session.y, session.z,
                    session.snapshot, current);
        }
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onInventoryClick(InventoryClickEvent e) {
        onInventoryClickInternal(e.getWhoClicked(), e.getInventory());
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onInventoryDrag(InventoryDragEvent e) {
        onInventoryClickInternal(e.getWhoClicked(), e.getInventory());
    }

    private void onInventoryClickInternal(HumanEntity who, Inventory inv) {
        if (!(who instanceof Player p)) return;
        if (!(inv.getHolder() instanceof Container container)) return;
        ContainerSession session = openContainers.get(p.getUniqueId());
        if (session == null) return;

        Block b = container.getBlock();

        Bukkit.getAsyncScheduler().runDelayed(this, (task) -> {
            Block currentBlock = b.getWorld().getBlockAt(b.getX(), b.getY(), b.getZ());
            if (!(currentBlock.getState() instanceof Container)) return;
            String current = serializeBlock(currentBlock);
            if (!current.equals(session.snapshot)) {
                addLog(session.player, session.world, session.x, session.y, session.z,
                        session.snapshot, current);
                session.snapshot = current;
            }
        }, 1, TimeUnit.MILLISECONDS);
    }

    // ==================== ПОРШНИ ====================
    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onPistonExtend(BlockPistonExtendEvent e) {
        for (Block b : e.getBlocks()) {
            addLog("Piston", b.getWorld().getName(), b.getX(), b.getY(), b.getZ(),
                    serializeBlock(b), "minecraft:air");
            Block target = b.getRelative(e.getDirection());
            addLog("Piston", target.getWorld().getName(), target.getX(), target.getY(), target.getZ(),
                    "minecraft:air", serializeBlock(b));
        }
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onPistonRetract(BlockPistonRetractEvent e) {
        for (Block b : e.getBlocks()) {
            addLog("Piston", b.getWorld().getName(), b.getX(), b.getY(), b.getZ(),
                    serializeBlock(b), "minecraft:air");
        }
    }

    // ==================== ОГОНЬ / РАСПРОСТРАНЕНИЕ ====================
    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onIgnite(BlockIgniteEvent e) {
        String actor = e.getPlayer() != null ? e.getPlayer().getName()
                : (e.getCause() != null ? "Ignite:" + e.getCause().name() : "Ignite");
        Block b = e.getBlock();
        addLog(actor, b.getWorld().getName(), b.getX(), b.getY(), b.getZ(),
                "minecraft:air", b.getBlockData().getAsString());
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBurn(BlockBurnEvent e) {
        Block b = e.getBlock();
        addLog("Fire", b.getWorld().getName(), b.getX(), b.getY(), b.getZ(),
                serializeBlock(b), "minecraft:air");
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockGrow(BlockGrowEvent e) {
        Block b = e.getBlock();
        addLog("Growth", b.getWorld().getName(), b.getX(), b.getY(), b.getZ(),
                b.getBlockData().getAsString(), e.getNewState().getBlockData().getAsString());
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockFade(BlockFadeEvent e) {
        Block b = e.getBlock();
        addLog("Fade", b.getWorld().getName(), b.getX(), b.getY(), b.getZ(),
                b.getBlockData().getAsString(), e.getNewState().getBlockData().getAsString());
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockSpread(BlockSpreadEvent e) {
        Block b = e.getBlock();
        addLog("Spread", b.getWorld().getName(), b.getX(), b.getY(), b.getZ(),
                b.getBlockData().getAsString(), e.getNewState().getBlockData().getAsString());
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockForm(BlockFormEvent e) {
        Block b = e.getBlock();
        addLog("Form", b.getWorld().getName(), b.getX(), b.getY(), b.getZ(),
                b.getBlockData().getAsString(), e.getNewState().getBlockData().getAsString());
    }

    // ==================== ВЁДРА ====================
    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBucketEmpty(PlayerBucketEmptyEvent e) {
        Block b = e.getBlock();
        String fluid = e.getBucket() == Material.WATER_BUCKET ? "minecraft:water" : "minecraft:lava";
        addLog(e.getPlayer().getName(), b.getWorld().getName(),
                b.getX(), b.getY(), b.getZ(),
                b.getBlockData().getAsString(), fluid);
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBucketFill(PlayerBucketFillEvent e) {
        Block b = e.getBlock();
        addLog(e.getPlayer().getName(), b.getWorld().getName(),
                b.getX(), b.getY(), b.getZ(),
                b.getBlockData().getAsString(), "minecraft:air");
    }

    // ==================== РАМКИ ====================
    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onHangingBreak(HangingBreakEvent e) {
        if (e.getEntity() instanceof Player p) {
            Entity h = e.getEntity();
            Location loc = h.getLocation();
            addLog(p.getName(), loc.getWorld().getName(),
                    loc.getBlockX(), loc.getBlockY(), loc.getBlockZ(),
                    "hanging:" + h.getType().name(), "minecraft:air");
        }
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onHangingPlace(HangingPlaceEvent e) {
        if (e.getPlayer() == null) return;
        Location loc = e.getEntity().getLocation();
        addLog(e.getPlayer().getName(), loc.getWorld().getName(),
                loc.getBlockX(), loc.getBlockY(), loc.getBlockZ(),
                "minecraft:air", "hanging:" + e.getEntity().getType().name());
    }

    // ==================== ДРОП ====================
    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onDrop(PlayerDropItemEvent e) {
        Item item = e.getItemDrop();
        Location loc = item.getLocation();
        String data = item.getItemStack().getType().name();
        addLog(e.getPlayer().getName(), loc.getWorld().getName(),
                loc.getBlockX(), loc.getBlockY(), loc.getBlockZ(),
                "minecraft:air", "item:" + data);
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onPickup(EntityPickupItemEvent e) {
        if (!(e.getEntity() instanceof Player p)) return;
        Item item = e.getItem();
        Location loc = item.getLocation();
        String data = item.getItemStack().getType().name();
        addLog(p.getName(), loc.getWorld().getName(),
                loc.getBlockX(), loc.getBlockY(), loc.getBlockZ(),
                "item:" + data, "minecraft:air");
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onEntityDeath(EntityDeathEvent e) {
        LivingEntity entity = e.getEntity();
        if (!entity.getType().isAlive()) return;
        if (entity.getType() == EntityType.ARMOR_STAND) {
            Location loc = entity.getLocation();
            addLog("Death:" + entity.getType().name(), loc.getWorld().getName(),
                    loc.getBlockX(), loc.getBlockY(), loc.getBlockZ(),
                    "entity:" + entity.getType().name(), "minecraft:air");
        }
    }

    // ==================== РОЛЛБЕК (ИСПРАВЛЕНО) ====================
    /**
     * Логика отката:
     *  1. Берём для каждого блока самую РАННЮЮ запись в диапазоне времени.
     *  2. Значение old_block из этой записи — это состояние блока ДО первого изменения.
     *     Именно его и нужно восстановить.
     *
     *  Пример: игрок поставил TNT-сферу и взорвал её.
     *     запись 1: old=air, new=tnt   (поставил)
     *     запись 2: old=tnt, new=air   (взрыв)
     *     Самый ранний old_block = air  → откат убирает TNT. ✓
     *
     *  Пример: игрок сломал алмазный блок и поставил камень.
     *     запись 1: old=almaз, new=air  (сломал)
     *     запись 2: old=air,   new=stone (поставил)
     *     Самый ранний old_block = алмаз → откат возвращает алмаз. ✓
     */
    private void handleRollback(Player p, int radiusChunks, long timeDelta) {
        forceFlushComplete();

        final World world = p.getWorld();
        final long endTime = System.currentTimeMillis();
        final long startTime = endTime - timeDelta;
        final Location center = p.getLocation();
        final int centerX = center.getBlockX();
        final int centerZ = center.getBlockZ();

        // Радиус в блоках: radiusChunks * 16
        final int radiusBlocks = radiusChunks * 16;
        final int minX = centerX - radiusBlocks;
        final int maxX = centerX + radiusBlocks;
        final int minZ = centerZ - radiusBlocks;
        final int maxZ = centerZ + radiusBlocks;

        p.sendMessage("§e[FR] Откат: радиус " + radiusChunks + " чанков (" + radiusBlocks + " блоков)");
        p.sendMessage("§7Период: с " + formatTime(timeDelta) + " назад по настоящее время");
        p.sendMessage("§7Границы: X[" + minX + ".." + maxX + "] Z[" + minZ + ".." + maxZ + "]");

        Bukkit.getAsyncScheduler().runNow(this, (task) -> {
            // ВАЖНО: берём MIN(time) и old_block из этой записи.
            // В SQLite это делается через подзапрос.
            String query =
                    "SELECT l.x, l.y, l.z, l.old_block " +
                            "FROM logs l " +
                            "INNER JOIN (" +
                            "   SELECT x, y, z, MIN(time) AS mt " +
                            "   FROM logs " +
                            "   WHERE world=? AND time > ? AND time <= ? " +
                            "   AND x BETWEEN ? AND ? AND z BETWEEN ? AND ? " +
                            "   GROUP BY x, y, z" +
                            ") sub ON l.x=sub.x AND l.y=sub.y AND l.z=sub.z AND l.time=sub.mt " +
                            "WHERE l.world=? AND l.time > ? AND l.time <= ? " +
                            "AND l.x BETWEEN ? AND ? AND l.z BETWEEN ? AND ?";

            List<BlockChange> changes = new ArrayList<>();

            synchronized (dbLock) {
                try (Connection conn = dataSource.getConnection();
                     PreparedStatement ps = conn.prepareStatement(query)) {
                    int i = 1;
                    ps.setString(i++, world.getName());
                    ps.setLong(i++, startTime);
                    ps.setLong(i++, endTime);
                    ps.setInt(i++, minX);
                    ps.setInt(i++, maxX);
                    ps.setInt(i++, minZ);
                    ps.setInt(i++, maxZ);

                    ps.setString(i++, world.getName());
                    ps.setLong(i++, startTime);
                    ps.setLong(i++, endTime);
                    ps.setInt(i++, minX);
                    ps.setInt(i++, maxX);
                    ps.setInt(i++, minZ);
                    ps.setInt(i, maxZ);

                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            String oldBlock = rs.getString("old_block");
                            if (oldBlock == null) continue;
                            changes.add(new BlockChange(
                                    rs.getInt("x"), rs.getInt("y"), rs.getInt("z"), oldBlock));
                        }
                    }
                } catch (SQLException ex) {
                    p.sendMessage("§cОшибка при чтении из БД: " + ex.getMessage());
                    getLogger().log(Level.SEVERE, "Rollback query error", ex);
                    return;
                }
            }

            if (changes.isEmpty()) {
                p.sendMessage("§c[FR] Не найдено изменений для отката");
                return;
            }

            p.sendMessage("§e[FR] Найдено " + changes.size() + " блоков для восстановления.");
            applyAllChanges(p, world, changes);
        });
    }

    private void applyAllChanges(Player p, World world, List<BlockChange> changes) {
        Map<String, List<BlockChange>> changesByChunk = new HashMap<>();
        for (BlockChange change : changes) {
            String key = (change.x >> 4) + "," + (change.z >> 4);
            changesByChunk.computeIfAbsent(key, k -> new ArrayList<>()).add(change);
        }

        final AtomicInteger totalApplied = new AtomicInteger(0);
        final AtomicInteger processedChunks = new AtomicInteger(0);
        final int totalChunks = changesByChunk.size();
        final AtomicBoolean completed = new AtomicBoolean(false);

        p.sendMessage("§7Обработка " + totalChunks + " чанков...");

        for (Map.Entry<String, List<BlockChange>> entry : changesByChunk.entrySet()) {
            String[] coords = entry.getKey().split(",");
            int chunkX = Integer.parseInt(coords[0]);
            int chunkZ = Integer.parseInt(coords[1]);
            List<BlockChange> chunkChanges = entry.getValue();

            getServer().getRegionScheduler().execute(this, world, chunkX, chunkZ, () -> {
                int appliedInChunk = 0;
                for (BlockChange change : chunkChanges) {
                    try {
                        deserializeAndPlace(world, change.x, change.y, change.z, change.oldBlock);
                        appliedInChunk++;
                    } catch (Exception ex) {
                        getLogger().warning("Ошибка при установке блока: " + ex.getMessage());
                    }
                }

                int applied = totalApplied.addAndGet(appliedInChunk);
                int processed = processedChunks.incrementAndGet();

                if (processed % 10 == 0 || processed == totalChunks) {
                    Bukkit.getAsyncScheduler().runNow(FlorestRollback.this, (n) ->
                            p.sendMessage("§7Прогресс: " + processed + "/" + totalChunks +
                                    " чанков, применено: " + applied + " блоков"));
                }

                if (processed == totalChunks && completed.compareAndSet(false, true)) {
                    totalRolledBackBlocks.addAndGet(applied);
                    Bukkit.getAsyncScheduler().runNow(FlorestRollback.this, (n) -> {
                        p.sendMessage("§a[FR] Откат завершен! Восстановлено: " + applied);
                    });
                    getLogger().info("Роллбек: " + p.getName() +
                            ", восстановлено=" + applied + ", найдено=" + changes.size());
                }
            });
        }
    }

    private void forceFlushComplete() {
        flushQueueSafe();
        int attempts = 0;
        while (isFlushing.get() && attempts < 200) {
            try { Thread.sleep(50); attempts++; } catch (InterruptedException ignored) {}
        }
    }

    private String formatTime(long millis) {
        if (millis < 60000) return (millis / 1000) + " сек.";
        if (millis < 3600000) return (millis / 60000) + " мин.";
        if (millis < 86400000) return (millis / 3600000) + " ч.";
        return (millis / 86400000) + " дн.";
    }

    // ==================== DEBUG ====================
    @SuppressWarnings("unused")
    private void debugDatabase(Player p, int x, int y, int z) {
        World world = p.getWorld();
        p.sendMessage("§6=== Отладка блока " + x + "," + y + "," + z + " ===");
        BlockData current = world.getBlockData(x, y, z);
        p.sendMessage("§7Текущий блок: §f" + current.getAsString());

        String query = "SELECT player, old_block, new_block, time FROM logs " +
                "WHERE x=? AND y=? AND z=? AND world=? ORDER BY time DESC LIMIT 10";

        synchronized (dbLock) {
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(query)) {
                ps.setInt(1, x); ps.setInt(2, y); ps.setInt(3, z);
                ps.setString(4, world.getName());
                ResultSet rs = ps.executeQuery();
                p.sendMessage("§7История изменений:");
                int count = 0;
                while (rs.next()) {
                    count++;
                    long ago = (System.currentTimeMillis() - rs.getLong("time")) / 1000;
                    p.sendMessage("§7  " + count + ". §e" + rs.getString("player") +
                            " §7изменил §f" + stripNbt(rs.getString("old_block")) +
                            " §7на §f" + stripNbt(rs.getString("new_block")) +
                            " §8(" + ago + " сек.)");
                }
                if (count == 0) p.sendMessage("§cИстория пуста!");
            } catch (SQLException ex) {
                p.sendMessage("§cОшибка БД: " + ex.getMessage());
            }
        }
    }

    // ==================== КОМАНДЫ ====================
    @Override
    public boolean onCommand(CommandSender sender, Command cmd, String label, String[] args) {
        if (!(sender instanceof Player p)) {
            sender.sendMessage("§cТолько для игроков");
            return true;
        }

        if (args.length == 0) { sendHelp(p); return true; }

        switch (args[0].toLowerCase()) {
            case "rollback": {
                int radius = 1;
                long time = TimeUnit.HOURS.toMillis(1);

                for (int i = 1; i < args.length; i++) {
                    if (args[i].startsWith("r:")) {
                        try { radius = Integer.parseInt(args[i].substring(2)); }
                        catch (NumberFormatException e) { p.sendMessage("§cНеверный радиус"); return true; }
                    } else if (args[i].startsWith("t:")) {
                        time = parseTime(args[i].substring(2));
                    }
                }

                if (radius < 1 || radius > 50) {
                    p.sendMessage("§cРадиус должен быть от 1 до 50 чанков");
                    return true;
                }

                handleRollback(p, radius, time);
                return true;
            }
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
            case "purge": {
                long t = (args.length > 1)
                        ? parseTime(args[1].replace("t:", ""))
                        : TimeUnit.DAYS.toMillis(30);
                executePurge(t);
                p.sendMessage("§aОчистка запущена в фоновом режиме.");
                return true;
            }
            case "stats":
                p.sendMessage("§6=== Статистика ===");
                p.sendMessage("§7Записано блоков: §e" + totalLoggedBlocks.get());
                p.sendMessage("§7Откачено блоков: §e" + totalRolledBackBlocks.get());
                p.sendMessage("§7Очередь: §e" + queueSize.get() + " / " + MAX_QUEUE);
                p.sendMessage("§7Кэш блоков: §e" + blockDataCache.size());
                p.sendMessage("§7Открытых контейнеров: §e" + openContainers.size());
                return true;
            case "debug":
                if (args.length == 4) {
                    try {
                        debugDatabase(p,
                                Integer.parseInt(args[1]),
                                Integer.parseInt(args[2]),
                                Integer.parseInt(args[3]));
                    } catch (NumberFormatException e) {
                        p.sendMessage("§cИспользование: /fr debug <x> <y> <z>");
                    }
                } else {
                    Block b = p.getLocation().getBlock();
                    debugDatabase(p, b.getX(), b.getY(), b.getZ());
                }
                return true;
            default:
                sendHelp(p);
                return true;
        }
    }

    @Override
    public List<String> onTabComplete(CommandSender sender, Command cmd, String alias, String[] args) {
        if (!(sender instanceof Player)) {
            return Collections.emptyList();
        }

        if (args.length == 1) {
            List<String> completions = Arrays.asList(
                    "rollback", "inspect", "i", "purge", "stats", "debug"
            );
            return completions.stream()
                    .filter(s -> s.toLowerCase().startsWith(args[0].toLowerCase()))
                    .toList();
        }

        if (args[0].equalsIgnoreCase("rollback")) {
            if (args.length == 2) {
                List<String> completions = Arrays.asList(
                        "r:1", "r:5", "r:10", "r:20", "r:30", "r:50",
                        "t:10m", "t:30m", "t:1h", "t:6h", "t:12h", "t:1d", "t:7d", "t:30d"
                );
                return completions.stream()
                        .filter(s -> s.toLowerCase().startsWith(args[1].toLowerCase()))
                        .toList();
            }

            if (args.length == 3) {
                List<String> completions = new ArrayList<>();
                if (args[1].toLowerCase().startsWith("r:")) {
                    completions.addAll(Arrays.asList(
                            "t:10m", "t:30m", "t:1h", "t:6h", "t:12h", "t:1d", "t:7d", "t:30d"
                    ));
                } else if (args[1].toLowerCase().startsWith("t:")) {
                    completions.addAll(Arrays.asList(
                            "r:1", "r:5", "r:10", "r:20", "r:30", "r:50"
                    ));
                }
                return completions.stream()
                        .filter(s -> s.toLowerCase().startsWith(args[2].toLowerCase()))
                        .toList();
            }
        }

        if (args[0].equalsIgnoreCase("purge") && args.length == 2) {
            List<String> completions = Arrays.asList(
                    "t:1h", "t:6h", "t:12h", "t:1d", "t:7d", "t:30d", "t:90d"
            );
            return completions.stream()
                    .filter(s -> s.toLowerCase().startsWith(args[1].toLowerCase()))
                    .toList();
        }

        return Collections.emptyList();
    }

    private void sendHelp(Player p) {
        p.sendMessage("§6=== FlorestRollback Help ===");
        p.sendMessage("§e/fr rollback r:<радиус> t:<время> §7- Откатить изменения");
        p.sendMessage("§e  §7Радиус в ЧАНКАХ. Пример: §f/fr rollback r:10 t:1h");
        p.sendMessage("§e/fr inspect §7- Режим просмотра истории блока");
        p.sendMessage("§e/fr purge t:30d §7- Очистить старые записи");
        p.sendMessage("§e/fr stats §7- Показать статистику");
        p.sendMessage("§e/fr debug [x y z] §7- Отладка блока");
    }

    private long parseTime(String input) {
        try {
            long num = Long.parseLong(input.replaceAll("[^0-9]", ""));
            if (input.endsWith("m")) return TimeUnit.MINUTES.toMillis(num);
            if (input.endsWith("h")) return TimeUnit.HOURS.toMillis(num);
            if (input.endsWith("d")) return TimeUnit.DAYS.toMillis(num);
            if (input.endsWith("s")) return TimeUnit.SECONDS.toMillis(num);
        } catch (Exception ignored) {}
        return TimeUnit.HOURS.toMillis(1);
    }

    // ==================== DATA CLASSES ====================
    private record LogEntry(String player, String world, int x, int y, int z,
                            String oldB, String newB, long time) {
        String toWal() {
            return esc(player) + "\u001F" + esc(world) + "\u001F" + x + "\u001F" + y + "\u001F" + z +
                    "\u001F" + esc(oldB) + "\u001F" + esc(newB) + "\u001F" + time;
        }

        static LogEntry fromWal(String line) {
            try {
                String[] p = line.split("\u001F", -1);
                if (p.length < 8) return null;
                return new LogEntry(unesc(p[0]), unesc(p[1]),
                        Integer.parseInt(p[2]), Integer.parseInt(p[3]), Integer.parseInt(p[4]),
                        unesc(p[5]), unesc(p[6]), Long.parseLong(p[7]));
            } catch (Exception e) {
                return null;
            }
        }

        private static String esc(String s) {
            if (s == null) return "";
            return s.replace("\u001F", "\\\u001F").replace("\n", "\\\n");
        }

        private static String unesc(String s) {
            return s.replace("\\\u001F", "\u001F").replace("\\\n", "\n");
        }
    }

    private static class ContainerSession {
        final String world;
        final int x, y, z;
        final String player;
        String snapshot;

        ContainerSession(String world, int x, int y, int z, String snapshot, String player) {
            this.world = world;
            this.x = x; this.y = y; this.z = z;
            this.snapshot = snapshot;
            this.player = player;
        }
    }

    private record BlockChange(int x, int y, int z, String oldBlock) {}
}