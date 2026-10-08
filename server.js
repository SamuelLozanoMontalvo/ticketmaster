const express = require('express');
const http = require('http');
const crypto = require('crypto');
const { Server } = require('socket.io');
const fs = require('fs');
const path = require('path');

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] }
});

app.use(express.static(__dirname));

// --- PERSISTENCIA EN ARCHIVO DATA.JSON ---
const DATA_FILE = path.join(__dirname, 'data.json');

let seatsState = {};
let salesHistory = [];
let staffUsers = [];
let adminSessions = {}; // token -> username
const activeTimers = {};
const LOCK_TIME_MS = 5 * 60 * 1000;
const TOTAL_CAPACITY = 600;

function cargarDatos() {
  try {
    if (fs.existsSync(DATA_FILE)) {
      const parsed = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
      seatsState = parsed.seatsState || {};
      salesHistory = parsed.salesHistory || [];
      staffUsers = parsed.staffUsers || [];
      adminSessions = parsed.adminSessions || {};
      console.log('Datos cargados exitosamente desde data.json');
    }
  } catch (err) {
    console.error('Error leyendo data.json:', err);
  }

  // Al reiniciar el servidor los timers se pierden: las sillas "locked" quedarían
  // pegadas para siempre. Se limpian y se reconstruyen las vendidas desde el historial.
  Object.keys(seatsState).forEach(id => {
    if (seatsState[id].status !== 'sold') delete seatsState[id];
  });
  salesHistory.forEach(v => v.puestos.forEach(id => { seatsState[id] = { status: 'sold' }; }));
}

function guardarDatos() {
  try {
    fs.writeFileSync(
      DATA_FILE,
      JSON.stringify({ seatsState, salesHistory, staffUsers, adminSessions }, null, 2),
      'utf8'
    );
  } catch (err) {
    console.error('Error guardando en data.json:', err);
  }
}

cargarDatos();

// Credenciales Administradores
const ADMIN_USERS = [
  { username: 'admin', password: 'admin123password' },
  { username: 'admindos', password: 'emhotelsadmin31' }
];

function adminPayload() {
  return { sales: salesHistory, staffList: staffUsers, totalCapacity: TOTAL_CAPACITY };
}

// Solo los administradores autenticados reciben los datos de ventas/staff
function broadcastAdminData() {
  io.to('admins').emit('ADMIN_DATA', adminPayload());
}

function isAdmin(socket) {
  return socket.data.admin === true;
}

io.on('connection', (socket) => {
  socket.emit('MAP_STATE', seatsState);

  socket.on('GET_MAP_STATE', () => socket.emit('MAP_STATE', seatsState));

  // --- BLOQUEAR SILLAS (TEMPORAL) ---
  socket.on('LOCK_SEATS', ({ seatIds }) => {
    if (!Array.isArray(seatIds)) return;
    const allAvailable = seatIds.every(id => !seatsState[id] || seatsState[id].status === 'available');

    if (!allAvailable) {
      socket.emit('LOCK_FAILED', { message: 'Una o más sillas seleccionadas ya no están disponibles.' });
      return;
    }

    const expiresAt = Date.now() + LOCK_TIME_MS;
    seatIds.forEach(seatId => {
      seatsState[seatId] = { status: 'locked', userId: socket.id, expiresAt };
      if (activeTimers[seatId]) clearTimeout(activeTimers[seatId]);

      activeTimers[seatId] = setTimeout(() => {
        if (seatsState[seatId] && seatsState[seatId].status === 'locked') {
          delete seatsState[seatId];
          delete activeTimers[seatId];
          guardarDatos();
          io.emit('SEATS_RELEASED', { seatIds: [seatId] });
        }
      }, LOCK_TIME_MS);
    });

    guardarDatos();
    io.emit('SEATS_LOCKED', { seatIds, userId: socket.id, expiresAt });
  });

  socket.on('UNLOCK_SEATS', ({ seatIds }) => {
    if (!Array.isArray(seatIds)) return;
    const unlocked = [];
    seatIds.forEach(seatId => {
      const s = seatsState[seatId];
      if (s && s.status === 'locked' && s.userId === socket.id) {
        delete seatsState[seatId];
        if (activeTimers[seatId]) { clearTimeout(activeTimers[seatId]); delete activeTimers[seatId]; }
        unlocked.push(seatId);
      }
    });
    if (unlocked.length > 0) {
      guardarDatos();
      io.emit('SEATS_RELEASED', { seatIds: unlocked });
    }
  });

  // --- COMPRA / APARTADO VENDIDO ---
  socket.on('CONFIRM_PURCHASE', (datosCompra) => {
    const seatIds = datosCompra && datosCompra.seatIds;
    if (!Array.isArray(seatIds) || seatIds.length === 0) return;

    // Solo se puede vender lo que este mismo cliente tiene bloqueado y sigue vigente
    const valid = seatIds.every(id => {
      const s = seatsState[id];
      return s && s.status === 'locked' && s.userId === socket.id;
    });

    if (!valid) {
      socket.emit('PURCHASE_FAILED', {
        message: 'Tu reserva expiró o las sillas ya no están disponibles. Vuelve a seleccionarlas.'
      });
      return;
    }

    seatIds.forEach(seatId => {
      if (activeTimers[seatId]) { clearTimeout(activeTimers[seatId]); delete activeTimers[seatId]; }
      seatsState[seatId] = { status: 'sold' };
    });

    salesHistory.push({
      idPedido: datosCompra.idPedido,
      codigoCompra: datosCompra.codigoCompra,
      cliente: datosCompra.cliente,
      numEntradas: seatIds.length,
      puestos: seatIds,
      total: datosCompra.total,
      fechaHora: datosCompra.fechaHora,
      usado: false,
      escaneadoPor: null,
      fechaEscaneo: null
    });
    guardarDatos();

    socket.emit('PURCHASE_OK', { codigoCompra: datosCompra.codigoCompra });
    io.emit('SEATS_SOLD', { seatIds });
    broadcastAdminData();
  });

  // --- CONSULTAR ENTRADAS DE CLIENTE ---
  socket.on('GET_USER_TICKETS', ({ cliente }) => {
    socket.emit('USER_TICKETS_RESPONSE', salesHistory.filter(v => v.cliente === cliente));
  });

  // --- LOGIN ADMIN ---
  socket.on('ADMIN_LOGIN', ({ username, password }) => {
    const foundAdmin = ADMIN_USERS.find(a => a.username === username && a.password === password);
    if (!foundAdmin) {
      socket.emit('ADMIN_AUTH_FAILED', { message: 'Credenciales de Administrador incorrectas.' });
      return;
    }
    const token = crypto.randomBytes(24).toString('hex');
    adminSessions[token] = foundAdmin.username;
    guardarDatos();

    socket.data.admin = true;
    socket.data.adminUser = foundAdmin.username;
    socket.join('admins');
    socket.emit('ADMIN_AUTH_SUCCESS', { username: foundAdmin.username, token, ...adminPayload() });
  });

  // Reanuda la sesión con el token (recarga de página o reconexión), sin reenviar la contraseña
  socket.on('ADMIN_RESUME', ({ token }) => {
    const username = adminSessions[token];
    if (!username) {
      socket.emit('ADMIN_SESSION_INVALID');
      return;
    }
    socket.data.admin = true;
    socket.data.adminUser = username;
    socket.join('admins');
    socket.emit('ADMIN_RESUMED', { username, ...adminPayload() });
  });

  socket.on('ADMIN_LOGOUT', ({ token }) => {
    if (token && adminSessions[token]) { delete adminSessions[token]; guardarDatos(); }
    socket.data.admin = false;
    socket.leave('admins');
  });

  // --- LOGIN STAFF ---
  socket.on('STAFF_LOGIN', ({ username, password }) => {
    const foundStaff = staffUsers.find(s => s.username === username && s.password === password);
    if (foundStaff) socket.emit('STAFF_AUTH_SUCCESS', foundStaff);
    else socket.emit('STAFF_AUTH_FAILED', { message: 'Credenciales de Staff no encontradas.' });
  });

  // --- ELIMINAR BOLETA VENDIDA (ADMIN) ---
  socket.on('DELETE_TICKET', ({ codigoCompra }) => {
    if (!isAdmin(socket)) {
      socket.emit('ADMIN_SESSION_INVALID');
      return;
    }
    const index = salesHistory.findIndex(v => v.codigoCompra === codigoCompra);
    if (index === -1) return;

    const ticket = salesHistory[index];
    ticket.puestos.forEach(seatId => {
      delete seatsState[seatId];
      if (activeTimers[seatId]) { clearTimeout(activeTimers[seatId]); delete activeTimers[seatId]; }
    });
    salesHistory.splice(index, 1);
    guardarDatos();

    io.emit('SEATS_RELEASED', { seatIds: ticket.puestos });
    broadcastAdminData();
  });

  // --- CREAR / ELIMINAR STAFF (ADMIN) ---
  socket.on('CREATE_STAFF', (staffData) => {
    if (!isAdmin(socket)) {
      socket.emit('ADMIN_SESSION_INVALID');
      return;
    }
    staffUsers.push({
      id: 'STF-' + crypto.randomBytes(3).toString('hex').toUpperCase(),
      nombre: staffData.nombre,
      apellido: staffData.apellido,
      username: staffData.username,
      password: staffData.password,
      fechaCreacion: new Date().toLocaleDateString('es-CO')
    });
    guardarDatos();
    broadcastAdminData();
  });

  socket.on('DELETE_STAFF', ({ staffId }) => {
    if (!isAdmin(socket)) {
      socket.emit('ADMIN_SESSION_INVALID');
      return;
    }
    staffUsers = staffUsers.filter(s => s.id !== staffId);
    guardarDatos();
    broadcastAdminData();
  });

  // --- VALIDACIÓN QR (STAFF) ---
  socket.on('VALIDATE_TICKET', ({ codigo, staffUsername }) => {
    const venta = salesHistory.find(v => v.codigoCompra === codigo || v.idPedido === codigo);

    if (!venta) {
      socket.emit('VALIDATION_RESULT', { status: 'INVALID', message: '❌ CÓDIGO NO ENCONTRADO EN EL SISTEMA' });
      return;
    }

    if (venta.usado) {
      socket.emit('VALIDATION_RESULT', {
        status: 'USED',
        message: `⚠️ ENTRADA YA FUE UTILIZADA\nEscaneado previamente por: ${venta.escaneadoPor} a las ${venta.fechaEscaneo}`
      });
      return;
    }

    venta.usado = true;
    venta.escaneadoPor = staffUsername;
    venta.fechaEscaneo = new Date().toLocaleTimeString('es-CO');
    guardarDatos();

    socket.emit('VALIDATION_RESULT', {
      status: 'VALID',
      message: `✅ ENTRADA VÁLIDA - ¡PUEDE PASAR!\nCliente: ${venta.cliente}\nPuestos: ${venta.puestos.join(', ')}`
    });
    broadcastAdminData();
  });

  socket.on('disconnect', () => {
    const released = [];
    Object.keys(seatsState).forEach(seatId => {
      const s = seatsState[seatId];
      if (s.status === 'locked' && s.userId === socket.id) {
        delete seatsState[seatId];
        if (activeTimers[seatId]) { clearTimeout(activeTimers[seatId]); delete activeTimers[seatId]; }
        released.push(seatId);
      }
    });
    if (released.length > 0) {
      guardarDatos();
      io.emit('SEATS_RELEASED', { seatIds: released });
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Servidor activo en el puerto ${PORT}`));
