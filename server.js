const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const fs = require('fs');
const path = require('path');

const app = express();
const server = http.createServer(app);

app.use(express.json());
app.use(express.static(__dirname));

const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] }
});

const DATA_FILE = path.join(__dirname, 'data.json');

let seatsState = {};
let salesHistory = [];
let staffUsers = [];
const activeTimers = {};
const LOCK_TIME_MS = 5 * 60 * 1000;

function cargarDatos() {
  try {
    if (fs.existsSync(DATA_FILE)) {
      const rawData = fs.readFileSync(DATA_FILE, 'utf8');
      const parsed = JSON.parse(rawData);
      seatsState = parsed.seatsState || {};
      salesHistory = parsed.salesHistory || [];
      staffUsers = parsed.staffUsers || [];

      // RECONSTRUCCIÓN DE SEGURIDAD:
      // Re-sincronizar el mapa seatsState con todas las compras históricas en salesHistory
      salesHistory.forEach(venta => {
        if (venta.puestos && Array.isArray(venta.puestos)) {
          venta.puestos.forEach(seatId => {
            seatsState[seatId] = { status: 'sold' };
          });
        }
      });
    }
  } catch (err) {
    console.error('Error leyendo data.json:', err);
  }
}

function guardarDatos() {
  try {
    fs.writeFileSync(DATA_FILE, JSON.stringify({ seatsState, salesHistory, staffUsers }, null, 2), 'utf8');
  } catch (err) {
    console.error('Error guardando en data.json:', err);
  }
}

cargarDatos();

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

// Lógica de Socket.io
io.on('connection', (socket) => {
  // Enviar el estado real y guardado del mapa a cada usuario nuevo o al refrescar
  socket.emit('MAP_STATE', seatsState);

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
        guardarDatos();
        io.emit('SEATS_RELEASED', { seatIds: [seatId] });
      }, LOCK_TIME_MS);
    });

    guardarDatos();
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
      guardarDatos();
      io.emit('SEATS_RELEASED', { seatIds: unlocked });
    }
  });

  socket.on('CONFIRM_PURCHASE', (datosCompra) => {
    if (!datosCompra || !datosCompra.seatIds || datosCompra.seatIds.length === 0) return;

    const etapa = obtenerEtapaServidor();
    let totalVerificado = 0;

    // REGISTRO PERMANENTE DE SILLAS VENDIDAS
    datosCompra.seatIds.forEach(seatId => {
      if (activeTimers[seatId]) clearTimeout(activeTimers[seatId]);
      
      // Se guarda como 'sold' en el objeto persistente
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

    salesHistory.push(nuevaVenta);
    
    // GUARDAR EN DISCO EN DATA.JSON
    guardarDatos();

    // EMITIR EVENTO EN TIEMPO REAL A TODOS
    io.emit('SEATS_SOLD', { seatIds: datosCompra.seatIds });
    io.emit('ADMIN_NEW_SALE', salesHistory);
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

  socket.on('DELETE_TICKET', ({ codigoCompra }) => {
    const index = salesHistory.findIndex(v => v.codigoCompra === codigoCompra);
    if (index !== -1) {
      const ticket = salesHistory[index];
      ticket.puestos.forEach(seatId => {
        delete seatsState[seatId];
        if (activeTimers[seatId]) clearTimeout(activeTimers[seatId]);
      });
      salesHistory.splice(index, 1);
      
      // Guardar cambios al liberar/eliminar
      guardarDatos();

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
    guardarDatos();
    io.emit('STAFF_LIST_UPDATED', staffUsers);
  });

  socket.on('DELETE_STAFF', ({ staffId }) => {
    staffUsers = staffUsers.filter(s => s.id !== staffId);
    guardarDatos();
    io.emit('STAFF_LIST_UPDATED', staffUsers);
  });

  socket.on('VALIDATE_TICKET', ({ codigo, staffUsername }) => {
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

    venta.usado = true;
    venta.escaneadoPor = staffUsername;
    venta.fechaEscaneo = new Date().toLocaleTimeString('es-CO');
    guardarDatos();

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
    if (userSeats.length > 0) {
      guardarDatos();
      io.emit('SEATS_RELEASED', { seatIds: userSeats });
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Servidor de Cena San Silvestre activo en puerto ${PORT}`));
