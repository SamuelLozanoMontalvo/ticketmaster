const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { Pool } = require('pg');

const app = express();
const server = http.createServer(app);

app.use(express.json());
app.use(express.static(__dirname));

const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] }
});

// Configuración de conexión a PostgreSQL
const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgresql://postgres:KkTyOoYeNvXtdFBPbMpChSDJhTwXiWrj@postgres.railway.internal:5432/railway',
  ssl: process.env.DATABASE_URL && !process.env.DATABASE_URL.includes('railway.internal') ? { rejectUnauthorized: false } : false
});

let seatsState = {};
let salesHistory = [];
let staffUsers = [];
const activeTimers = {};
const LOCK_TIME_MS = 5 * 60 * 1000;

// Creación de tablas e inicialización en PostgreSQL
async function initDB() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS ventas (
        id SERIAL PRIMARY KEY,
        id_pedido VARCHAR(50) UNIQUE NOT NULL,
        codigo_compra VARCHAR(50) UNIQUE NOT NULL,
        cliente VARCHAR(100) NOT NULL,
        num_entradas INT NOT NULL,
        puestos TEXT[] NOT NULL,
        total INT NOT NULL,
        fecha_hora VARCHAR(100) NOT NULL,
        usado BOOLEAN DEFAULT FALSE,
        escaneado_por VARCHAR(100),
        fecha_escaneo VARCHAR(100)
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
    console.log('✅ Tablas de PostgreSQL listas.');
    await cargarDatosDB();
  } catch (err) {
    console.error('❌ Error inicializando PostgreSQL:', err);
  }
}

async function cargarDatosDB() {
  try {
    const salesRes = await pool.query('SELECT * FROM ventas ORDER BY id ASC');
    salesHistory = salesRes.rows.map(r => ({
      idPedido: r.id_pedido,
      codigoCompra: r.codigo_compra,
      cliente: r.cliente,
      numEntradas: r.num_entradas,
      puestos: r.puestos,
      total: r.total,
      fechaHora: r.fecha_hora,
      usado: r.usado,
      escaneadoPor: r.escaneado_por,
      fechaEscaneo: r.fecha_escaneo
    }));

    const staffRes = await pool.query('SELECT * FROM staff');
    staffUsers = staffRes.rows.map(r => ({
      id: r.id,
      nombre: r.nombre,
      apellido: r.apellido,
      username: r.username,
      password: r.password,
      fechaCreacion: r.fecha_creacion
    }));

    // Sincronizar el estado permanente del mapa en memoria
    seatsState = {};
    salesHistory.forEach(venta => {
      if (venta.puestos && Array.isArray(venta.puestos)) {
        venta.puestos.forEach(seatId => {
          seatsState[seatId] = { status: 'sold' };
        });
      }
    });

    console.log(`[PostgreSQL] Datos restaurados: ${salesHistory.length} ventas activas.`);
  } catch (err) {
    console.error('❌ Error leyendo registros de PostgreSQL:', err);
  }
}

initDB();

const ADMIN_USERS = [
  { username: 'admin', password: 'admin123password' },
  { username: 'admindos', password: 'emhotelsadmin31' }
];

const TABLA_PRECIOS_SERVER = {
  PREVENTA: {
    SAHARA: { ADULTO: 1050000, NINO: 420000 },
    OASIS:  { ADULTO: 950000,  NINO: 420000 },
    NOMAD:  { ADULTO: 850000,  NINO: 420000 }
  },
  ETAPA2: {
    SAHARA: { ADULTO: 1100000, NINO: 490000 },
    OASIS:  { ADULTO: 1000000, NINO: 490000 },
    NOMAD:  { ADULTO: 900000,  NINO: 490000 }
  },
  FULL: {
    SAHARA: { ADULTO: 1200000, NINO: 550000 },
    OASIS:  { ADULTO: 1100000, NINO: 550000 },
    NOMAD:  { ADULTO: 1000000, NINO: 550000 }
  }
};

function obtenerEtapaServidor() {
  const hoy = new Date();
  const mes = hoy.getMonth() + 1;
  const dia = hoy.getDate();

  if (mes < 10 || (mes === 10 && dia <= 15)) return 'PREVENTA';
  if ((mes === 10 && dia >= 16) || mes === 11) return 'ETAPA2';
  return 'FULL';
}

function obtenerZonaPuesto(seatId) {
  const letra = seatId.charAt(0);
  if (letra === 'A' || letra === 'B') return 'SAHARA';
  if (['C', 'D', 'E', 'F', 'G', 'H'].includes(letra)) return 'OASIS';
  return 'NOMAD';
}

function enviarEstadoConsolidado(targetSocket) {
  salesHistory.forEach(v => {
    if (v.puestos) {
      v.puestos.forEach(s => {
        seatsState[s] = { status: 'sold' };
      });
    }
  });

  if (targetSocket) {
    targetSocket.emit('MAP_STATE', seatsState);
  } else {
    io.emit('MAP_STATE', seatsState);
  }
}

io.on('connection', (socket) => {
  enviarEstadoConsolidado(socket);

  socket.on('GET_MAP_STATE', () => {
    enviarEstadoConsolidado(socket);
  });

  socket.on('LOCK_SEATS', ({ seatIds }) => {
    const expiresAt = Date.now() + LOCK_TIME_MS;
    const allAvailable = seatIds.every(id => !seatsState[id] || seatsState[id].status === 'available');

    if (!allAvailable) {
      socket.emit('LOCK_FAILED', { message: 'Puestos no disponibles.' });
      return;
    }

    seatIds.forEach(seatId => {
      seatsState[seatId] = { status: 'locked', userId: socket.id, expiresAt };
      if (activeTimers[seatId]) clearTimeout(activeTimers[seatId]);

      activeTimers[seatId] = setTimeout(() => {
        delete seatsState[seatId];
        delete activeTimers[seatId];
        io.emit('SEATS_RELEASED', { seatIds: [seatId] });
      }, LOCK_TIME_MS);
    });

    io.emit('SEATS_LOCKED', { seatIds, userId: socket.id, expiresAt });
  });

  socket.on('UNLOCK_SEATS', ({ seatIds }) => {
    const unlocked = [];
    seatIds.forEach(seatId => {
      if (seatsState[seatId] && seatsState[seatId].userId === socket.id) {
        delete seatsState[seatId];
        if (activeTimers[seatId]) clearTimeout(activeTimers[seatId]);
        unlocked.push(seatId);
      }
    });
    if (unlocked.length > 0) {
      io.emit('SEATS_RELEASED', { seatIds: unlocked });
    }
  });

  socket.on('CONFIRM_PURCHASE', async (datosCompra) => {
    if (!datosCompra || !datosCompra.seatIds || datosCompra.seatIds.length === 0) return;

    const etapa = obtenerEtapaServidor();
    let totalVerificado = 0;

    datosCompra.seatIds.forEach(seatId => {
      if (activeTimers[seatId]) clearTimeout(activeTimers[seatId]);
      seatsState[seatId] = { status: 'sold' };

      const zona = obtenerZonaPuesto(seatId);
      totalVerificado += TABLA_PRECIOS_SERVER[etapa][zona].ADULTO;
    });

    const nuevaVenta = {
      idPedido: datosCompra.idPedido || "ORD-" + Math.floor(100000 + Math.random() * 900000),
      codigoCompra: datosCompra.codigoCompra || "TK-" + Math.random().toString(36).substring(2, 8).toUpperCase(),
      cliente: datosCompra.cliente || 'Cliente General',
      numEntradas: datosCompra.seatIds.length,
      puestos: datosCompra.seatIds,
      total: datosCompra.total || totalVerificado,
      fechaHora: datosCompra.fechaHora || new Date().toLocaleString('es-CO'),
      usado: false,
      escaneadoPor: null,
      fechaEscaneo: null
    };

    try {
      await pool.query(
        `INSERT INTO ventas (id_pedido, codigo_compra, cliente, num_entradas, puestos, total, fecha_hora)
         VALUES ($1, $2, $3, $4, $5, $6, $7)`,
        [nuevaVenta.idPedido, nuevaVenta.codigoCompra, nuevaVenta.cliente, nuevaVenta.numEntradas, nuevaVenta.puestos, nuevaVenta.total, nuevaVenta.fechaHora]
      );
      salesHistory.push(nuevaVenta);

      io.emit('SEATS_SOLD', { seatIds: datosCompra.seatIds });
      io.emit('ADMIN_NEW_SALE', salesHistory);
    } catch (err) {
      console.error('Error insertando venta en PostgreSQL:', err);
    }
  });

  socket.on('GET_USER_TICKETS', ({ cliente }) => {
    socket.emit('USER_TICKETS_RESPONSE', salesHistory.filter(v => v.cliente === cliente));
  });

  socket.on('ADMIN_LOGIN', ({ username, password }) => {
    const foundAdmin = ADMIN_USERS.find(a => a.username === username && a.password === password);
    if (foundAdmin) {
      socket.emit('ADMIN_AUTH_SUCCESS', {
        username: foundAdmin.username,
        sales: salesHistory,
        staffList: staffUsers,
        totalCapacity: 600
      });
    } else {
      socket.emit('ADMIN_AUTH_FAILED', { message: 'Credenciales de Administrador incorrectas.' });
    }
  });

  socket.on('STAFF_LOGIN', ({ username, password }) => {
    const foundStaff = staffUsers.find(s => s.username === username && s.password === password);
    if (foundStaff) socket.emit('STAFF_AUTH_SUCCESS', foundStaff);
    else socket.emit('STAFF_AUTH_FAILED', { message: 'Credenciales de Staff no encontradas.' });
  });

  socket.on('DELETE_TICKET', async ({ codigoCompra }) => {
    try {
      const index = salesHistory.findIndex(v => v.codigoCompra === codigoCompra);
      if (index !== -1) {
        const ticket = salesHistory[index];
        await pool.query('DELETE FROM ventas WHERE codigo_compra = $1', [codigoCompra]);

        ticket.puestos.forEach(seatId => {
          delete seatsState[seatId];
          if (activeTimers[seatId]) clearTimeout(activeTimers[seatId]);
        });
        salesHistory.splice(index, 1);

        io.emit('SEATS_RELEASED', { seatIds: ticket.puestos });
        io.emit('ADMIN_NEW_SALE', salesHistory);
      }
    } catch (err) {
      console.error('Error eliminando ticket en PostgreSQL:', err);
    }
  });

  socket.on('CREATE_STAFF', async (staffData) => {
    const newStaff = {
      id: 'STF-' + Math.floor(1000 + Math.random() * 9000),
      nombre: staffData.nombre,
      apellido: staffData.apellido,
      username: staffData.username,
      password: staffData.password,
      fechaCreacion: new Date().toLocaleDateString('es-CO')
    };

    try {
      await pool.query(
        `INSERT INTO staff (id, nombre, apellido, username, password, fecha_creacion)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [newStaff.id, newStaff.nombre, newStaff.apellido, newStaff.username, newStaff.password, newStaff.fechaCreacion]
      );
      staffUsers.push(newStaff);
      io.emit('STAFF_LIST_UPDATED', staffUsers);
    } catch (err) {
      console.error('Error creando staff en PostgreSQL:', err);
    }
  });

  socket.on('DELETE_STAFF', async ({ staffId }) => {
    try {
      await pool.query('DELETE FROM staff WHERE id = $1', [staffId]);
      staffUsers = staffUsers.filter(s => s.id !== staffId);
      io.emit('STAFF_LIST_UPDATED', staffUsers);
    } catch (err) {
      console.error('Error eliminando staff en PostgreSQL:', err);
    }
  });

  socket.on('VALIDATE_TICKET', async ({ codigo, staffUsername }) => {
    const venta = salesHistory.find(v => v.codigoCompra === codigo || v.idPedido === codigo);

    if (!venta) {
      socket.emit('VALIDATION_RESULT', { status: 'INVALID', message: '❌ CÓDIGO NO ENCONTRADO EN EL SISTEMA' });
      return;
    }

    if (venta.usado) {
      socket.emit('VALIDATION_RESULT', {
        status: 'USED',
        message: `⚠️ ENTRADA YA FUE UTILIZADA\nEscaneado por: ${venta.escaneadoPor} a las ${venta.fechaEscaneo}`
      });
      return;
    }

    const fechaEscaneo = new Date().toLocaleTimeString('es-CO');
    try {
      await pool.query(
        'UPDATE ventas SET usado = TRUE, escaneado_por = $1, fecha_escaneo = $2 WHERE codigo_compra = $3 OR id_pedido = $3',
        [staffUsername, fechaEscaneo, codigo]
      );

      venta.usado = true;
      venta.escaneadoPor = staffUsername;
      venta.fechaEscaneo = fechaEscaneo;

      socket.emit('VALIDATION_RESULT', {
        status: 'VALID',
        message: `✅ ENTRADA VÁLIDA - ¡PUEDE PASAR!\nCliente: ${venta.cliente}\nPuestos: ${venta.puestos.join(', ')}`
      });

      io.emit('ADMIN_NEW_SALE', salesHistory);
    } catch (err) {
      console.error('Error validando ticket en PostgreSQL:', err);
    }
  });

  socket.on('disconnect', () => {
    const userSeats = [];
    Object.keys(seatsState).forEach(seatId => {
      if (seatsState[seatId].userId === socket.id && seatsState[seatId].status === 'locked') {
        delete seatsState[seatId];
        if (activeTimers[seatId]) clearTimeout(activeTimers[seatId]);
        userSeats.push(seatId);
      }
    });
    if (userSeats.length > 0) {
      io.emit('SEATS_RELEASED', { seatIds: userSeats });
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Servidor activo en el puerto ${PORT}`));
