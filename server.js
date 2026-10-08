const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { Pool } = require('pg');

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] }
});

app.use(express.static(__dirname));

// --- CONEXIÓN A POSTGRESQL (Railway) ---
const connectionString = process.env.DATABASE_URL || 'postgresql://postgres:PyLgqHzbzvUeNnzITenwIpMfoVqnzXaW@postgres.railway.internal:5432/railway';

const pool = new Pool({
  connectionString,
  ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
});

// Inicialización de la base de datos y creación automática de tablas
async function initDB() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS seats (
        id VARCHAR(20) PRIMARY KEY,
        status VARCHAR(20) NOT NULL,
        user_id VARCHAR(100),
        expires_at BIGINT
      );

      CREATE TABLE IF NOT EXISTS sales (
        codigo_compra VARCHAR(50) PRIMARY KEY,
        id_pedido VARCHAR(50) NOT NULL,
        cliente VARCHAR(100) NOT NULL,
        num_entradas INT NOT NULL,
        puestos TEXT[] NOT NULL,
        total INT NOT NULL,
        fecha_hora VARCHAR(50) NOT NULL,
        usado BOOLEAN DEFAULT FALSE,
        escaneado_por VARCHAR(100),
        fecha_escaneo VARCHAR(50)
      );

      CREATE TABLE IF NOT EXISTS staff (
        id VARCHAR(50) PRIMARY KEY,
        nombre VARCHAR(100) NOT NULL,
        apellido VARCHAR(100) NOT NULL,
        username VARCHAR(100) UNIQUE NOT NULL,
        password VARCHAR(100) NOT NULL,
        fecha_creacion VARCHAR(50) NOT NULL
      );
    `);
    console.log('✅ Base de datos PostgreSQL conectada y tablas verificadas.');
  } catch (err) {
    console.error('❌ Error inicializando PostgreSQL:', err);
  }
}

initDB();

const ADMIN_USERS = [
  { username: 'admin', password: 'admin123password' },
  { username: 'admindos', password: 'emhotelsadmin31' }
];

const LOCK_TIME_MS = 5 * 60 * 1000;
const activeTimers = {};

// Funciones auxiliares para consultar BBDD
async function getMapState() {
  const res = await pool.query('SELECT id, status, user_id, expires_at FROM seats');
  const map = {};
  res.rows.forEach(r => {
    map[r.id] = { status: r.status, userId: r.user_id, expiresAt: Number(r.expires_at) };
  });
  return map;
}

async function getSalesHistory() {
  const res = await pool.query('SELECT codigo_compra AS "codigoCompra", id_pedido AS "idPedido", cliente, num_entradas AS "numEntradas", puestos, total, fecha_hora AS "fechaHora", usado, escaneado_por AS "escaneadoPor", fecha_escaneo AS "fechaEscaneo" FROM sales ORDER BY fecha_hora DESC');
  return res.rows;
}

async function getStaffUsers() {
  const res = await pool.query('SELECT id, nombre, apellido, username, password, fecha_creacion AS "fechaCreacion" FROM staff');
  return res.rows;
}

io.on('connection', async (socket) => {
  // Enviar estado actual al conectar
  const currentMap = await getMapState();
  socket.emit('MAP_STATE', currentMap);

  // --- BLOQUEAR SILLAS ---
  socket.on('LOCK_SEATS', async ({ seatIds }) => {
    const expiresAt = Date.now() + LOCK_TIME_MS;
    try {
      const res = await pool.query('SELECT id, status FROM seats WHERE id = ANY($1)', [seatIds]);
      const taken = res.rows.filter(r => r.status === 'sold' || (r.status === 'locked' && r.id !== socket.id));

      if (taken.length > 0) {
        socket.emit('LOCK_FAILED', { message: 'Una o más sillas ya no están disponibles.' });
        return;
      }

      for (const seatId of seatIds) {
        await pool.query(
          `INSERT INTO seats (id, status, user_id, expires_at)
           VALUES ($1, 'locked', $2, $3)
           ON CONFLICT (id) DO UPDATE SET status = 'locked', user_id = $2, expires_at = $3`,
          [seatId, socket.id, expiresAt]
        );

        if (activeTimers[seatId]) clearTimeout(activeTimers[seatId]);

        activeTimers[seatId] = setTimeout(async () => {
          await pool.query("DELETE FROM seats WHERE id = $1 AND status = 'locked'", [seatId]);
          delete activeTimers[seatId];
          io.emit('SEATS_RELEASED', { seatIds: [seatId] });
        }, LOCK_TIME_MS);
      }

      io.emit('SEATS_LOCKED', { seatIds, userId: socket.id, expiresAt });
    } catch (err) {
      console.error('Error en LOCK_SEATS:', err);
    }
  });

  // --- DESBLOQUEAR SILLAS ---
  socket.on('UNLOCK_SEATS', async ({ seatIds }) => {
    try {
      await pool.query("DELETE FROM seats WHERE id = ANY($1) AND user_id = $2 AND status = 'locked'", [seatIds, socket.id]);
      seatIds.forEach(seatId => {
        if (activeTimers[seatId]) clearTimeout(activeTimers[seatId]);
      });
      io.emit('SEATS_RELEASED', { seatIds });
    } catch (err) {
      console.error('Error en UNLOCK_SEATS:', err);
    }
  });

  // --- CONFIRMAR COMPRA / APARTAR ---
  socket.on('CONFIRM_PURCHASE', async (datosCompra) => {
    if (!datosCompra || !datosCompra.seatIds || datosCompra.seatIds.length === 0) return;

    try {
      for (const seatId of datosCompra.seatIds) {
        if (activeTimers[seatId]) clearTimeout(activeTimers[seatId]);
        await pool.query(
          `INSERT INTO seats (id, status, user_id, expires_at)
           VALUES ($1, 'sold', NULL, NULL)
           ON CONFLICT (id) DO UPDATE SET status = 'sold', user_id = NULL, expires_at = NULL`,
          [seatId]
        );
      }

      const idPedido = datosCompra.idPedido || "ORD-" + Math.floor(100000 + Math.random() * 900000);
      const codigoCompra = datosCompra.codigoCompra || "TK-" + Math.random().toString(36).substring(2, 8).toUpperCase();
      const cliente = datosCompra.cliente || 'Cliente General';
      const fechaHora = datosCompra.fechaHora || new Date().toLocaleString('es-CO');

      await pool.query(
        `INSERT INTO sales (codigo_compra, id_pedido, cliente, num_entradas, puestos, total, fecha_hora, usado)
         VALUES ($1, $2, $3, $4, $5, $6, $7, FALSE)`,
        [codigoCompra, idPedido, cliente, datosCompra.seatIds.length, datosCompra.seatIds, datosCompra.total || 0, fechaHora]
      );

      const updatedSales = await getSalesHistory();
      io.emit('SEATS_SOLD', { seatIds: datosCompra.seatIds });
      io.emit('ADMIN_NEW_SALE', updatedSales);
    } catch (err) {
      console.error('Error en CONFIRM_PURCHASE:', err);
    }
  });

  // --- CONSULTAR ENTRADAS CLIENTE ---
  socket.on('GET_USER_TICKETS', async ({ cliente }) => {
    try {
      const res = await pool.query(
        'SELECT codigo_compra AS "codigoCompra", puestos, fecha_hora AS "fechaHora", total, usado FROM sales WHERE cliente = $1',
        [cliente]
      );
      socket.emit('USER_TICKETS_RESPONSE', res.rows);
    } catch (err) {
      console.error('Error en GET_USER_TICKETS:', err);
    }
  });

  // --- LOGIN ADMIN ---
  socket.on('ADMIN_LOGIN', async ({ username, password }) => {
    const foundAdmin = ADMIN_USERS.find(a => a.username === username && a.password === password);
    if (foundAdmin) {
      const sales = await getSalesHistory();
      const staffList = await getStaffUsers();
      socket.emit('ADMIN_AUTH_SUCCESS', {
        username: foundAdmin.username,
        sales,
        staffList,
        totalCapacity: 600
      });
    } else {
      socket.emit('ADMIN_AUTH_FAILED', { message: 'Credenciales de Administrador incorrectas.' });
    }
  });

  // --- LOGIN STAFF ---
  socket.on('STAFF_LOGIN', async ({ username, password }) => {
    try {
      const res = await pool.query('SELECT id, nombre, apellido, username FROM staff WHERE username = $1 AND password = $2', [username, password]);
      if (res.rows.length > 0) {
        socket.emit('STAFF_AUTH_SUCCESS', res.rows[0]);
      } else {
        socket.emit('STAFF_AUTH_FAILED', { message: 'Credenciales de Staff no encontradas.' });
      }
    } catch (err) {
      console.error('Error en STAFF_LOGIN:', err);
    }
  });

  // --- ELIMINAR BOLETA VENDIDA ---
  socket.on('DELETE_TICKET', async ({ codigoCompra }) => {
    try {
      const res = await pool.query('SELECT puestos FROM sales WHERE codigo_compra = $1', [codigoCompra]);
      if (res.rows.length > 0) {
        const puestos = res.rows[0].puestos;
        await pool.query('DELETE FROM seats WHERE id = ANY($1)', [puestos]);
        await pool.query('DELETE FROM sales WHERE codigo_compra = $1', [codigoCompra]);

        puestos.forEach(sId => {
          if (activeTimers[sId]) clearTimeout(activeTimers[sId]);
        });

        const updatedSales = await getSalesHistory();
        io.emit('SEATS_RELEASED', { seatIds: puestos });
        io.emit('ADMIN_NEW_SALE', updatedSales);
      }
    } catch (err) {
      console.error('Error en DELETE_TICKET:', err);
    }
  });

  // --- CREAR Y ELIMINAR STAFF ---
  socket.on('CREATE_STAFF', async (staffData) => {
    try {
      const newId = 'STF-' + Math.floor(1000 + Math.random() * 9000);
      const fecha = new Date().toLocaleDateString('es-CO');
      await pool.query(
        'INSERT INTO staff (id, nombre, apellido, username, password, fecha_creacion) VALUES ($1, $2, $3, $4, $5, $6)',
        [newId, staffData.nombre, staffData.apellido, staffData.username, staffData.password, fecha]
      );
      const staffList = await getStaffUsers();
      io.emit('STAFF_LIST_UPDATED', staffList);
    } catch (err) {
      console.error('Error en CREATE_STAFF:', err);
    }
  });

  socket.on('DELETE_STAFF', async ({ staffId }) => {
    try {
      await pool.query('DELETE FROM staff WHERE id = $1', [staffId]);
      const staffList = await getStaffUsers();
      io.emit('STAFF_LIST_UPDATED', staffList);
    } catch (err) {
      console.error('Error en DELETE_STAFF:', err);
    }
  });

  // --- VALIDAR QR (STAFF) ---
  socket.on('VALIDATE_TICKET', async ({ codigo, staffUsername }) => {
    try {
      const res = await pool.query('SELECT * FROM sales WHERE codigo_compra = $1 OR id_pedido = $1', [codigo]);
      if (res.rows.length === 0) {
        socket.emit('VALIDATION_RESULT', { status: 'INVALID', message: '❌ CÓDIGO NO ENCONTRADO EN EL SISTEMA' });
        return;
      }

      const venta = res.rows[0];
      if (venta.usado) {
        socket.emit('VALIDATION_RESULT', {
          status: 'USED',
          message: `⚠️ ENTRADA YA FUE UTILIZADA\nEscaneado por: ${venta.escaneado_por} a las ${venta.fecha_escaneo}`
        });
        return;
      }

      const fechaEscaneo = new Date().toLocaleTimeString('es-CO');
      await pool.query(
        'UPDATE sales SET usado = TRUE, escaneado_por = $1, fecha_escaneo = $2 WHERE codigo_compra = $3',
        [staffUsername, fechaEscaneo, venta.codigo_compra]
      );

      socket.emit('VALIDATION_RESULT', {
        status: 'VALID',
        message: `✅ ENTRADA VÁLIDA - ¡PUEDE PASAR!\nCliente: ${venta.cliente}\nPuestos: ${venta.puestos.join(', ')}`
      });

      const updatedSales = await getSalesHistory();
      io.emit('ADMIN_NEW_SALE', updatedSales);
    } catch (err) {
      console.error('Error en VALIDATE_TICKET:', err);
    }
  });

  // --- DESCONEXIÓN ---
  socket.on('disconnect', async () => {
    try {
      const res = await pool.query("SELECT id FROM seats WHERE user_id = $1 AND status = 'locked'", [socket.id]);
      if (res.rows.length > 0) {
        const releasedIds = res.rows.map(r => r.id);
        await pool.query("DELETE FROM seats WHERE user_id = $1 AND status = 'locked'", [socket.id]);
        releasedIds.forEach(sId => {
          if (activeTimers[sId]) clearTimeout(activeTimers[sId]);
        });
        io.emit('SEATS_RELEASED', { seatIds: releasedIds });
      }
    } catch (err) {
      console.error('Error en disconnect:', err);
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Servidor PostgreSQL activo en puerto ${PORT}`));
