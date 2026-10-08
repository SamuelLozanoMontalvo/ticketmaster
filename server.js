const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  }
});

app.use(express.static(__dirname));

// Estado en memoria
const seatsState = {};
const activeTimers = {};
const LOCK_TIME_MS = 5 * 60 * 1000;

let salesHistory = [];
let staffUsers = [];

// Lista de Administradores
const ADMIN_USERS = [
  { username: 'admin', password: 'admin123password' },
  { username: 'admindos', password: 'emhotelsadmin31' }
];

io.on('connection', (socket) => {
  socket.emit('MAP_STATE', seatsState);

  // --- GESTIÓN DE SILLAS Y BLOQUEOS ---
  socket.on('LOCK_SEATS', ({ seatIds }) => {
    const expiresAt = Date.now() + LOCK_TIME_MS;
    const allAvailable = seatIds.every(id => !seatsState[id] || seatsState[id].status === 'available');

    if (!allAvailable) {
      socket.emit('LOCK_FAILED', { message: 'Una o más sillas seleccionadas ya no están disponibles.' });
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
    if (unlocked.length > 0) io.emit('SEATS_RELEASED', { seatIds: unlocked });
  });

  // --- CONFIRMACIÓN Y REGISTRO DE COMPRA ---
  socket.on('CONFIRM_PURCHASE', (datosCompra) => {
    datosCompra.seatIds.forEach(seatId => {
      seatsState[seatId] = { status: 'sold' };
    });

    const nuevaVenta = {
      idPedido: datosCompra.idPedido,
      codigoCompra: datosCompra.codigoCompra,
      cliente: datosCompra.cliente,
      numEntradas: datosCompra.seatIds.length,
      puestos: datosCompra.seatIds,
      total: datosCompra.total,
      fechaHora: datosCompra.fechaHora,
      usado: false,
      escaneadoPor: null,
      fechaEscaneo: null
    };

    salesHistory.push(nuevaVenta);
    io.emit('SEATS_SOLD', { seatIds: datosCompra.seatIds });
    io.emit('ADMIN_NEW_SALE', salesHistory);
  });

  // --- CONSULTAR BOLETAS DE USUARIO ---
  socket.on('GET_USER_TICKETS', ({ cliente }) => {
    const userTickets = salesHistory.filter(v => v.cliente === cliente);
    socket.emit('USER_TICKETS_RESPONSE', userTickets);
  });

  // --- AUTENTICACIÓN ADMIN Y STAFF ---
  socket.on('ADMIN_LOGIN', ({ username, password }) => {
    const isAdminValid = ADMIN_USERS.some(a => a.username === username && a.password === password);
    if (isAdminValid) {
      socket.emit('ADMIN_AUTH_SUCCESS', {
        username,
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
    if (foundStaff) {
      socket.emit('STAFF_AUTH_SUCCESS', foundStaff);
    } else {
      socket.emit('STAFF_AUTH_FAILED', { message: 'Credenciales de Staff no encontradas.' });
    }
  });

  // --- ACCIONES DE ADMINISTRADOR (ELIMINAR ENTRADAS Y STAFF) ---
  socket.on('DELETE_TICKET', ({ codigoCompra }) => {
    const ticketIndex = salesHistory.findIndex(v => v.codigoCompra === codigoCompra);
    if (ticketIndex !== -1) {
      const ticket = salesHistory[ticketIndex];
      // Liberar los puestos en el mapa
      ticket.puestos.forEach(seatId => {
        delete seatsState[seatId];
      });
      salesHistory.splice(ticketIndex, 1);

      io.emit('SEATS_RELEASED', { seatIds: ticket.puestos });
      io.emit('ADMIN_NEW_SALE', salesHistory);
    }
  });

  socket.on('CREATE_STAFF', (staffData) => {
    const newStaff = {
      id: 'STF-' + Math.floor(1000 + Math.random() * 9000),
      nombre: staffData.nombre,
      apellido: staffData.apellido,
      username: staffData.username,
      password: staffData.password,
      fechaCreacion: new Date().toLocaleDateString('es-CO')
    };

    staffUsers.push(newStaff);
    io.emit('STAFF_LIST_UPDATED', staffUsers);
  });

  socket.on('DELETE_STAFF', ({ staffId }) => {
    staffUsers = staffUsers.filter(s => s.id !== staffId);
    io.emit('STAFF_LIST_UPDATED', staffUsers);
  });

  // --- VALIDACIÓN DE ENTRADAS (STAFF) ---
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

    socket.emit('VALIDATION_RESULT', {
      status: 'VALID',
      message: `✅ ENTRADA VÁLIDA - ¡PUEDE PASAR!\nCliente: ${venta.cliente}\nPuestos: ${venta.puestos.join(', ')}`
    });

    io.emit('ADMIN_NEW_SALE', salesHistory);
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
    if (userSeats.length > 0) io.emit('SEATS_RELEASED', { seatIds: userSeats });
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Servidor activo en el puerto ${PORT}`));
