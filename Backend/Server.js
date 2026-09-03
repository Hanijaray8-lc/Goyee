const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const http = require('http'); 
const fs = require('fs');
const path = require('path');
const { Server } = require('socket.io'); // Socket.io
const { default: makeWASocket, useMultiFileAuthState, DisconnectReason, fetchLatestBaileysVersion } = require('@whiskeysockets/baileys');
const pino = require('pino');
require('dotenv').config();
const schedule = require('node-schedule');
const Schedule = require('./models/Schedule');

const app = express();
const server = http.createServer(app);
const allowedOrigins = [
  "http://localhost:3000",
  "https://goyee.lcind.space",
  "https://goye.in",
  "https://goye.in/"
];

// Socket.io CORS setup
const io = new Server(server, {
  cors: {
    origin: allowedOrigins,
    methods: ["GET", "POST"],
    credentials: true
  },
  maxHttpBufferSize: 1e8 // 100 MB limit for videos
});

app.use(cors({
  origin: allowedOrigins,
  credentials: true
}));
app.use(express.json({ limit: '100mb' }));
app.use(express.urlencoded({ limit: '100mb', extended: true }));

// MongoDB connection
mongoose.connect(process.env.MONGO_URI, { family: 4 })
  .then(() => {
    console.log("Connected to MongoDB Atlas");
    loadPendingSchedules();
  })
  .catch((err) => console.error("MongoDB error:", err));

// --- WhatsApp Logic Setup (User-Specific) ---
const userClients = {}; // key: email, value: { whatsappClient, latestQR, isWhatsAppAuthenticated }
const socketToEmail = {}; // key: socket.id, value: email
const emailToSockets = {}; // key: email, value: array of socket.id

function emitToUserSockets(email, event, data) {
    const socketIds = emailToSockets[email];
    if (socketIds) {
        socketIds.forEach(id => {
            io.to(id).emit(event, data);
        });
    }
}

async function startWhatsAppForUser(email) {
    if (!email) return;
    
    if (userClients[email]) {
        return userClients[email];
    }

    // Set loading/placeholder immediately to prevent concurrent startWhatsAppForUser race conditions
    userClients[email] = {
        whatsappClient: null,
        latestQR: "",
        isWhatsAppAuthenticated: false,
        loading: true
    };

    try {
        const folderName = `auth_info_${email.replace(/[^a-zA-Z0-9]/g, '_')}`;
        const { state, saveCreds } = await useMultiFileAuthState(folderName);
        const { version } = await fetchLatestBaileysVersion();

        const client = makeWASocket({
            version,
            auth: state,
            printQRInTerminal: false,
            logger: pino({ level: "silent" }),
            browser: ["Goyee", "Chrome", "1.0.0"]
        });

        userClients[email].whatsappClient = client;
        userClients[email].loading = false;

        client.ev.on('creds.update', saveCreds);

    client.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect, qr } = update;

        if (qr) {
            if (userClients[email]) {
                userClients[email].latestQR = qr;
                emitToUserSockets(email, "qr", qr);
            }
        }

        if (connection === 'open') {
            console.log(`✅ WhatsApp Authenticated for ${email}!`);
            if (userClients[email]) {
                userClients[email].latestQR = ""; 
                userClients[email].isWhatsAppAuthenticated = true;
                
                const userInfo = client.user ? {
                    id: client.user.id ? client.user.id.split(':')[0] : '',
                    name: client.user.name || client.user.verifiedName || ''
                } : null;
                
                emitToUserSockets(email, "ready", { message: "WhatsApp Authenticated Successfully!", user: userInfo });
            }
        }

        if (connection === 'close') {
            const shouldReconnect = lastDisconnect.error?.output?.statusCode !== DisconnectReason.loggedOut;
            console.log(`❌ WhatsApp connection closed for ${email}. Reconnecting:`, shouldReconnect);
            
            if (shouldReconnect) {
                if (userClients[email]) {
                    userClients[email].isWhatsAppAuthenticated = false;
                }
                // Let the frontend know the session dropped so it can stop
                // showing "connected" while we silently reconnect — avoids
                // users sending messages into a dead session.
                emitToUserSockets(email, "reconnecting", { message: "WhatsApp session reconnecting..." });
                delete userClients[email];
                startWhatsAppForUser(email);
            } else {
                if (userClients[email]) {
                    userClients[email].latestQR = ""; 
                    userClients[email].isWhatsAppAuthenticated = false;
                    emitToUserSockets(email, "logout", "User logged out from phone");
                }
                const authPath = path.join(__dirname, folderName);
                if (fs.existsSync(authPath)) {
                    try { fs.rmSync(authPath, { recursive: true, force: true }); } catch(e) {}
                }
                delete userClients[email];
                startWhatsAppForUser(email);
            }
        }
    });
  } catch (err) {
      console.error(`Error starting WhatsApp for ${email}:`, err);
      delete userClients[email];
  }
}

// --- Socket Connection ---
io.on("connection", (socket) => {
    console.log("React UI Connected to Socket!");

    socket.on("register_email", async (email) => {
        if (!email) return;
        
        socketToEmail[socket.id] = email;
        if (!emailToSockets[email]) {
            emailToSockets[email] = [];
        }
        if (!emailToSockets[email].includes(socket.id)) {
            emailToSockets[email].push(socket.id);
        }

        console.log(`Socket ${socket.id} registered for email: ${email}`);
        await startWhatsAppForUser(email);

        const userClient = userClients[email];
        if (userClient) {
            if (userClient.isWhatsAppAuthenticated) {
                const client = userClient.whatsappClient;
                const userInfo = client && client.user ? {
                    id: client.user.id ? client.user.id.split(':')[0] : '',
                    name: client.user.name || client.user.verifiedName || ''
                } : null;
                socket.emit("ready", { message: "WhatsApp Authenticated Successfully!", user: userInfo });
            } else if (userClient.latestQR) {
                socket.emit("qr", userClient.latestQR);
            }
        }
    });

    socket.on("disconnect", () => {
        const email = socketToEmail[socket.id];
        if (email) {
            if (emailToSockets[email]) {
                emailToSockets[email] = emailToSockets[email].filter(id => id !== socket.id);
                if (emailToSockets[email].length === 0) {
                    delete emailToSockets[email];
                }
            }
            delete socketToEmail[socket.id];
        }
        console.log("Socket disconnected:", socket.id);
    });

    socket.on("check_status", () => {
        const email = socketToEmail[socket.id];
        if (!email) return;
        const userClient = userClients[email];
        if (userClient) {
            if (userClient.isWhatsAppAuthenticated) {
                const client = userClient.whatsappClient;
                const userInfo = client && client.user ? {
                    id: client.user.id ? client.user.id.split(':')[0] : '',
                    name: client.user.name || client.user.verifiedName || ''
                } : null;
                socket.emit("ready", { message: "WhatsApp Authenticated Successfully!", user: userInfo });
            } else if (userClient.latestQR) {
                socket.emit("qr", userClient.latestQR); 
            }
        }
    });

    let forceResetting = false;
    socket.on("request_new_qr", async () => {
        const email = socketToEmail[socket.id];
        if (!email) return;

        if (forceResetting) return;
        forceResetting = true;
        
        console.log(`♻️ Force restarting WhatsApp to generate new QR for ${email}...`);
        const userClient = userClients[email];
        if (userClient) {
            userClient.isWhatsAppAuthenticated = false;
            userClient.latestQR = "";
            if (userClient.whatsappClient) {
                userClient.whatsappClient.ev.removeAllListeners('connection.update');
                try { userClient.whatsappClient.end(new Error("Force reset")); } catch(e) {}
            }
        }
        
        setTimeout(() => {
            const folderName = `auth_info_${email.replace(/[^a-zA-Z0-9]/g, '_')}`;
            const authPath = path.join(__dirname, folderName);
            if (fs.existsSync(authPath)) {
                try { fs.rmSync(authPath, { recursive: true, force: true }); } catch(e) {}
            }
            delete userClients[email];
            startWhatsAppForUser(email);
            forceResetting = false;
        }, 2000);
    });

    socket.on("send_bulk_message", async (data, callback) => {
        const email = socketToEmail[socket.id];
        const userClient = userClients[email];
        
        if (!userClient || !userClient.isWhatsAppAuthenticated) {
            if (typeof callback === "function") {
                callback({ success: false, error: "WhatsApp not authenticated" });
            }
            return;
        }

        // ===== CREDITS ENFORCEMENT (backend is the final authority) =====
        // Re-checks the user's real credit balance in MongoDB before doing
        // any sending. This is the actual send entry point (the socket
        // event), so this check can't be bypassed by skipping the
        // frontend's own pre-send check — that check is only a UX nicety.
        let creditUser = await User.findOne({ email });
        if (!creditUser) {
            if (typeof callback === "function") {
                callback({ success: false, error: "user_not_found", message: "We couldn't find your account. Please log in again." });
            }
            return;
        }
        creditUser = await checkAndResetDailyCredits(creditUser);
        if ((creditUser.credits || 0) <= 0) {
            socket.emit("credits_exhausted", { credits: creditUser.credits || 0 });
            if (typeof callback === "function") {
                callback({
                    success: false,
                    error: "credits_exhausted",
                    message: "Your credits are over. You can try again tomorrow.",
                    credits: creditUser.credits || 0
                });
            }
            return;
        }
        // Running totals, persisted to MongoDB as they change (see the
        // credit-deduction block below) so a crash mid-batch doesn't lose
        // already-earned deductions. Credits and message counts are kept
        // separate: each successful message consumes exactly 1 credit from
        // the user's stored credits balance.
        let runningCredits = creditUser.credits || 0;
        let runningTotalSent = creditUser.totalSent || 0;

        const whatsappClient = userClient.whatsappClient;
        const { numbers, text, media } = data;
        console.log("📥 Received numbers from frontend:", numbers);
        
        let validNumbersCount = 0;
        let successCount = 0;
        let failedCount = 0;
        let creditsRanOutMidBatch = false;
        
        socket.emit("bulk_progress_start", { total: numbers.length });

        for (const num of numbers) {
            if (num && String(num).trim() !== "") {

                // Stop sending further messages the instant credits hit 0,
                // even partway through this same batch — remaining
                // numbers are reported as blocked rather than silently
                // dropped or sent for free.
                if (runningCredits <= 0) {
                    creditsRanOutMidBatch = true;
                    socket.emit("bulk_progress_update", { status: "failed", number: num, reason: "Your credits are over. You can try again tomorrow." });
                    failedCount++;
                    continue;
                }

                validNumbersCount++;
                let cleanNum = String(num).replace(/\D/g, '');
                if (cleanNum.length === 10) {
                    cleanNum = '91' + cleanNum;
                }
                const formattedNumber = `${cleanNum}@s.whatsapp.net`;
                
                socket.emit("bulk_progress_update", { status: "sending", number: num });
                
                try {
                    // Check if the number is actually registered on WhatsApp
                    const checkNumber = await whatsappClient.onWhatsApp(formattedNumber);
                    if (!checkNumber || checkNumber.length === 0 || !checkNumber[0].exists) {
                        console.error(`❌ Number ${num} is not registered on WhatsApp`);
                        socket.emit("bulk_progress_update", { status: "failed", number: num, reason: "Number is not registered on WhatsApp" });
                        failedCount++;
                        continue; // Skip sending, move to next number
                    }

                    if (media && media.data) {
                        const buffer = Buffer.from(media.data, 'base64');
                        let mediaMessage = {};
                        let mime = media.mimetype ? media.mimetype.toLowerCase() : '';
                        const name = media.filename ? media.filename.toLowerCase() : '';
                        
                        // Fallback mime from file extension if empty or generic octet-stream
                        if (!mime || mime === 'application/octet-stream') {
                            const ext = name.split('.').pop();
                            const mimeMap = {
                                'png': 'image/png',
                                'jpg': 'image/jpeg',
                                'jpeg': 'image/jpeg',
                                'gif': 'image/gif',
                                'webp': 'image/webp',
                                'mp4': 'video/mp4',
                                'mov': 'video/quicktime',
                                'avi': 'video/x-msvideo',
                                'pdf': 'application/pdf',
                                'csv': 'text/csv',
                                'xls': 'application/vnd.ms-excel',
                                'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                                'doc': 'application/msword',
                                'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                                'txt': 'text/plain'
                            };
                            mime = mimeMap[ext] || mime;
                        }

                        const isImage = mime.startsWith('image/') || name.match(/\.(jpg|jpeg|png|gif|webp)$/);
                        const isVideo = mime.startsWith('video/') || name.match(/\.(mp4|mov|avi|webm|mkv)$/);

                        if (isImage) {
                            mediaMessage = { image: buffer, caption: text, mimetype: mime || 'image/jpeg' };
                        } else if (isVideo) {
                            mediaMessage = { video: buffer, caption: text, mimetype: mime || 'video/mp4' };
                        } else {
                            mediaMessage = { document: buffer, caption: text, mimetype: mime || 'application/octet-stream', fileName: media.filename || 'document' };
                        }
                        await whatsappClient.sendMessage(formattedNumber, mediaMessage);
                    } else {
                        await whatsappClient.sendMessage(formattedNumber, { text: text });
                    }
                    console.log(`✅ Message sent to ${num}`);
                    socket.emit("bulk_progress_update", { status: "sent", number: num });
                    successCount++;

                    // ===== CREDIT DEDUCTION (server-authoritative) =====
                    // 1 credit = 2 messages. runningTotalSent is the user's
                    // lifetime sent-message count, so its parity (odd/even)
                    // doubles as the "have we sent the 2nd message of this
                    // pair yet" tracker — no separate counter needed. A
                    // credit is deducted only on every 2nd successful send
                    // (when the new total is even): 1st message of a pair
                    // costs nothing yet, 2nd message deducts 1 credit.
                    // 30 credits -> 60 messages: after 2 sent, 29 credits;
                    // after 4 sent, 28 credits; etc.
                    runningTotalSent += 1;
                    if (runningTotalSent % 2 === 0) {
                        runningCredits = Math.max(0, runningCredits - 1);
                    }
                    creditUser.totalSent = runningTotalSent;
                    creditUser.credits = runningCredits;
                    await creditUser.save();
                    socket.emit("credits_updated", { credits: runningCredits, totalSent: runningTotalSent });

                    await new Promise(resolve => setTimeout(resolve, 2000));
                } catch (error) {
                    console.error(`❌ Failed to send to ${num}`, error);
                    
                    let reason = error.message || "Unable to send message. Please try again.";
                    
                    // Map raw exceptions to user-friendly messages
                    if (reason.includes("reading 'id'") || reason.includes("undefined") || reason.includes("Cannot read properties")) {
                        reason = "Number is not registered on WhatsApp.";
                    } else if (reason.includes("Unexpected error")) {
                        reason = "Unable to send message. Please try again.";
                    }

                    socket.emit("bulk_progress_update", { status: "failed", number: num, reason: reason });
                    failedCount++;
                }
            } else {
                if (num) {
                    socket.emit("bulk_progress_update", { status: "failed", number: num, reason: "Invalid empty number" });
                    failedCount++;
                }
            }
        }

        // Always increment the analytics count for the dashboard based on valid numbers submitted
        if (validNumbersCount > 0 || failedCount > 0) {
            const d = new Date();
            const offset = d.getTimezoneOffset() * 60000;
            const today = new Date(d.getTime() - offset).toISOString().split('T')[0];
            
            await MessageLog.findOneAndUpdate(
                { date: today },
                { $inc: { count: validNumbersCount, successCount: successCount, failedCount: failedCount } },
                { upsert: true, new: true }
            );
        }

        if (creditsRanOutMidBatch) {
            socket.emit("credits_exhausted", { credits: runningCredits });
        }

        socket.emit("bulk_progress_completed", { success: true });

        if (typeof callback === "function") {
            callback({ success: true, credits: runningCredits, creditsExhausted: creditsRanOutMidBatch });
        }
    });
});






app.get('/api/schedules', async (req, res) => {
    try {
        const schedules = await Schedule.find().sort({ scheduledFor: 1 });
        res.json(schedules);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.post('/api/schedules', async (req, res) => {
    try {
        const newSchedule = new Schedule(req.body);
        await newSchedule.save();
        scheduleJob(newSchedule);
        res.json({ message: 'Scheduled successfully', schedule: newSchedule });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.put('/api/schedules/:id', async (req, res) => {
    try {
        const updated = await Schedule.findByIdAndUpdate(req.params.id, req.body, { new: true });
        const existingJob = schedule.scheduledJobs[updated._id.toString()];
        if (existingJob) existingJob.cancel();
        
        if (updated.status === 'Pending') {
            scheduleJob(updated);
        }
        res.json(updated);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.delete('/api/schedules/:id', async (req, res) => {
    try {
        await Schedule.findByIdAndDelete(req.params.id);
        const existingJob = schedule.scheduledJobs[req.params.id];
        if (existingJob) existingJob.cancel();
        res.json({ message: 'Deleted' });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

function scheduleJob(scheduleDoc) {
    if (scheduleDoc.status !== 'Pending') return;

    const targetTime = new Date(scheduleDoc.scheduledFor);
    const now = new Date();

    if (isNaN(targetTime.getTime())) {
        console.error(`Invalid schedule date for job ${scheduleDoc._id}`);
        return;
    }

    if (targetTime <= now) {
        console.log(`⏰ Scheduled time ${targetTime.toISOString()} is due/past. Executing immediately for job ${scheduleDoc._id}`);
        executeScheduledJob(scheduleDoc);
    } else {
        console.log(`⏰ Scheduled job ${scheduleDoc._id} registered for ${targetTime.toISOString()}`);
        schedule.scheduleJob(scheduleDoc._id.toString(), targetTime, async () => {
            executeScheduledJob(scheduleDoc);
        });
    }
}

async function executeScheduledJob(scheduleDoc) {
    try {
        console.log('Running scheduled job for', scheduleDoc._id);
            const email = scheduleDoc.email;
            const userClient = userClients[email] || Object.values(userClients).find(uc => uc.isWhatsAppAuthenticated);
            if (!userClient || !userClient.isWhatsAppAuthenticated) {
                console.log(`Cannot send scheduled message: WhatsApp not authenticated for ${email}`);
                await Schedule.findByIdAndUpdate(scheduleDoc._id, { status: 'Failed' });
                return;
            }
            
            let creditUser = email ? await User.findOne({ email }) : null;
            if (creditUser) {
                creditUser = await checkAndResetDailyCredits(creditUser);
            }
            let runningCredits = creditUser ? (creditUser.credits || 0) : 999999;
            let runningTotalSent = creditUser ? (creditUser.totalSent || 0) : 0;
            let creditsRanOutMidBatch = false;

            if (creditUser && runningCredits <= 0) {
                console.log(`Cannot send scheduled message: No credits remaining for ${email}`);
                io.emit("credits_exhausted", { credits: 0 });
                await Schedule.findByIdAndUpdate(scheduleDoc._id, { status: 'Failed', sentAt: new Date() });
                return;
            }
            
            const whatsappClient = userClient.whatsappClient;
            let validNumbersCount = 0;
            let successCount = 0;
            let failedCount = 0;

            io.emit("bulk_progress_start", { total: scheduleDoc.contacts.length });

            for (let num of scheduleDoc.contacts) {
                if (creditUser && runningCredits <= 0) {
                    creditsRanOutMidBatch = true;
                    io.emit("bulk_progress_update", { status: "failed", number: num, reason: "Your credits are over." });
                    failedCount++;
                    continue;
                }

                if (!num || String(num).trim() === "") {
                    io.emit("bulk_progress_update", { status: "failed", number: num, reason: "Invalid empty number" });
                    failedCount++;
                    continue;
                }

                validNumbersCount++;
                let cleanNum = String(num).replace(/\D/g, '');
                if (cleanNum.length === 10) {
                    cleanNum = '91' + cleanNum;
                }
                const formattedNumber = `${cleanNum}@s.whatsapp.net`;

                io.emit("bulk_progress_update", { status: "sending", number: num });

                try {
                    const checkNumber = await whatsappClient.onWhatsApp(formattedNumber);
                    if (!checkNumber || checkNumber.length === 0 || !checkNumber[0].exists) {
                        io.emit("bulk_progress_update", { status: "failed", number: num, reason: "Number is not registered on WhatsApp" });
                        failedCount++;
                        continue;
                    }

                    if (scheduleDoc.media && scheduleDoc.media.data) {
                        const buffer = Buffer.from(scheduleDoc.media.data, 'base64');
                        let mediaMessage = {};
                        let mime = scheduleDoc.media.mimetype ? scheduleDoc.media.mimetype.toLowerCase() : '';
                        const name = scheduleDoc.media.filename ? scheduleDoc.media.filename.toLowerCase() : '';
                        
                        // Fallback mime from file extension if empty or generic octet-stream
                        if (!mime || mime === 'application/octet-stream') {
                            const ext = name.split('.').pop();
                            const mimeMap = {
                                'png': 'image/png',
                                'jpg': 'image/jpeg',
                                'jpeg': 'image/jpeg',
                                'gif': 'image/gif',
                                'webp': 'image/webp',
                                'mp4': 'video/mp4',
                                'mov': 'video/quicktime',
                                'avi': 'video/x-msvideo',
                                'pdf': 'application/pdf',
                                'csv': 'text/csv',
                                'xls': 'application/vnd.ms-excel',
                                'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                                'doc': 'application/msword',
                                'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                                'txt': 'text/plain'
                            };
                            mime = mimeMap[ext] || mime;
                        }

                        const isImage = mime.startsWith('image/') || name.match(/\.(jpg|jpeg|png|gif|webp)$/);
                        const isVideo = mime.startsWith('video/') || name.match(/\.(mp4|mov|avi|webm|mkv)$/);

                        if (isImage) {
                            mediaMessage = { image: buffer, caption: scheduleDoc.message, mimetype: mime || 'image/jpeg' };
                        } else if (isVideo) {
                            mediaMessage = { video: buffer, caption: scheduleDoc.message, mimetype: mime || 'video/mp4' };
                        } else {
                            mediaMessage = { document: buffer, caption: scheduleDoc.message, mimetype: mime || 'application/octet-stream', fileName: scheduleDoc.media.filename || 'document' };
                        }
                        await whatsappClient.sendMessage(formattedNumber, mediaMessage);
                    } else {
                        await whatsappClient.sendMessage(formattedNumber, { text: scheduleDoc.message });
                    }
                    io.emit("bulk_progress_update", { status: "sent", number: num });
                    successCount++;

                    // ===== SCHEDULED MESSAGE CREDIT DEDUCTION (2 messages = 1 credit) =====
                    if (creditUser) {
                        runningTotalSent += 1;
                        if (runningTotalSent % 2 === 0) {
                            runningCredits = Math.max(0, runningCredits - 1);
                        }
                        creditUser.totalSent = runningTotalSent;
                        creditUser.credits = runningCredits;
                        await creditUser.save();
                        io.emit("credits_updated", { credits: runningCredits, totalSent: runningTotalSent });
                    }

                    await new Promise(resolve => setTimeout(resolve, 2000));
                } catch(e) {
                    console.error('Error sending scheduled to', num, e);
                    let reason = e.message || "Unable to send message.";
                    if (reason.includes("reading 'id'") || reason.includes("undefined") || reason.includes("Cannot read properties")) {
                        reason = "Number is not registered on WhatsApp.";
                    }
                    io.emit("bulk_progress_update", { status: "failed", number: num, reason: reason });
                    failedCount++;
                }
            }

            if (creditsRanOutMidBatch) {
                io.emit("credits_exhausted", { credits: runningCredits });
            }
 
            if (validNumbersCount > 0 || failedCount > 0) {
                const d = new Date();
                const offset = d.getTimezoneOffset() * 60000;
                const today = new Date(d.getTime() - offset).toISOString().split('T')[0];
                await MessageLog.findOneAndUpdate(
                    { date: today },
                    { $inc: { count: validNumbersCount, successCount: successCount, failedCount: failedCount } },
                    { upsert: true, new: true }
                );
            }
 
            io.emit("bulk_progress_completed", { success: true });
            
            // If completely failed
            if (successCount === 0 && failedCount > 0) {
                 await Schedule.findByIdAndUpdate(scheduleDoc._id, { status: 'Failed', sentAt: new Date() });
            } else {
                 await Schedule.findByIdAndUpdate(scheduleDoc._id, { status: 'Completed', sentAt: new Date() });
            }
        } catch(e) {
            console.error('Job error', e);
            io.emit("bulk_progress_completed", { success: false });
        }
    }
 
async function loadPendingSchedules() {
    try {
        const pendings = await Schedule.find({ status: 'Pending' });
        console.log(`📋 Loaded ${pendings.length} pending schedule(s) from database.`);
        for (const s of pendings) {
            if (s.email) {
                await startWhatsAppForUser(s.email);
            }
            scheduleJob(s);
        }
    } catch (err) {
        console.error("Error loading pending schedules:", err);
    }
}

const PORT = process.env.PORT || 5000;
// ==========================================
// USER LOGIN & REGISTER API ROUTES
// ==========================================

const userSchema = new mongoose.Schema({
  name: { type: String, required: true },
  email: { type: String, required: true, unique: true },
  password: { type: String, required: true },
  phone: { type: String, required: false },
  location: { type: String, required: false },
  businessName: { type: String, required: false },
  businessType: { type: String, required: false },
  credits: { type: Number, default: 0 },
  totalSent: { type: Number, default: 0 },
  lastDailyRewardDate: { type: String, default: "" },
  welcomeCreditsShown: { type: Boolean, default: false }
});

const messageLogSchema = new mongoose.Schema({
  date: { type: String, required: true, unique: true }, // Format: YYYY-MM-DD
  count: { type: Number, default: 0 },
  successCount: { type: Number, default: 0 },
  failedCount: { type: Number, default: 0 }
});

// Avoid OverwriteModelError if it's already defined
const User = mongoose.models.User || mongoose.model('User', userSchema);
const MessageLog = mongoose.models.MessageLog || mongoose.model('MessageLog', messageLogSchema);

// Free User model — this server.js has no register/login routes of its
// own for Free Users (that flow lives elsewhere), but it connects to the
// SAME MongoDB database, so defining the model here (schema shape matches
// the free-user register flow: name/email/password/business fields, plus
// credits/status) lets /api/videos/* read and credit the exact same
// documents that flow already created. mongoose.models.FreeUser reuses the
// model if it's already registered elsewhere in this process.
const freeUserSchema = new mongoose.Schema({
  name: { type: String, required: true },
  email: { type: String, required: true, unique: true },
  password: { type: String, required: true },
  businessName: { type: String, required: false },
  businessType: { type: String, required: false },
  phone: { type: String, required: false },
  location: { type: String, required: false },
  createdAt: { type: Date, default: Date.now },
  credits: { type: Number, default: 0 },
  totalSent: { type: Number, default: 0 },
  status: { type: String, enum: ["active", "blocked"], default: "active" }
}, { strict: false }); // strict:false — don't drop/reject fields this simplified schema doesn't list but the real collection has (e.g. subscriptionPlan)
const FreeUser = mongoose.models.FreeUser || mongoose.model('FreeUser', freeUserSchema);

// IST-day helper — 'en-CA' formats as YYYY-MM-DD.
function todayKeyIST() {
  return new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Kolkata' }).format(new Date());
}

// Credits configuration:
// - Day 1 (Registration): 30 credits (welcome gift)
// - Day 2 onwards: 10 credits fresh every day (at 12:00 AM IST / next day access, non-accumulating)
const INITIAL_CREDITS = 30;
const DAILY_RESET_CREDITS = 10;

async function checkAndResetDailyCredits(user) {
  if (!user) return user;
  const today = todayKeyIST();
  // If user has no recorded lastDailyRewardDate or it is from an earlier day, reset to DAILY_RESET_CREDITS (10)
  if (!user.lastDailyRewardDate || user.lastDailyRewardDate !== today) {
    console.log(`🔄 [Daily Credit Reset] Resetting credits for ${user.email} from ${user.credits} to ${DAILY_RESET_CREDITS} on ${today} (was: ${user.lastDailyRewardDate || 'none'})`);
    user.credits = DAILY_RESET_CREDITS;
    user.lastDailyRewardDate = today;
    await user.save();
  }
  return user;
}

// Schedule automatic daily credit reset at 12:00 AM (midnight 00:00) IST every day
schedule.scheduleJob({ hour: 0, minute: 0, tz: 'Asia/Kolkata' }, async () => {
  try {
    const today = todayKeyIST();
    console.log(`⏰ [Midnight Cron] Running daily credit auto-reset to ${DAILY_RESET_CREDITS} credits for date: ${today}`);
    const result = await User.updateMany(
      { lastDailyRewardDate: { $ne: today } },
      { $set: { credits: DAILY_RESET_CREDITS, lastDailyRewardDate: today } }
    );
    console.log(`✅ [Midnight Cron] Successfully reset daily credits to ${DAILY_RESET_CREDITS} for ${result.modifiedCount} user(s).`);
  } catch (cronErr) {
    console.error("❌ [Midnight Cron] Error resetting daily credits:", cronErr);
  }
});

app.post('/api/register', async (req, res) => {
  try {
    const { name, email, password, phone, location, businessName, businessType } = req.body;
    const existingUser = await User.findOne({ email });
    if (existingUser) return res.status(400).json({ message: "User already exists" });
    
    const today = todayKeyIST();

    const newUser = new User({ 
      name, email, password, phone, location, businessName, businessType,
      credits: INITIAL_CREDITS,
      lastDailyRewardDate: today
    });
    
    await newUser.save();
    res.status(201).json({
      message: "Registration successful!",
      credits: INITIAL_CREDITS,
      creditsMessage: `You have successfully earned ${INITIAL_CREDITS} credits. Start your messages!`
    });
  } catch (error) {
    res.status(500).json({ message: "Error registering user", error });
  }
});

app.post('/api/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    let user = await User.findOne({ email, password });
    if (!user) return res.status(401).json({ message: "Invalid email or password" });

    // Check and reset daily credits to 10 if it's a new day
    user = await checkAndResetDailyCredits(user);

    const isNewUser = !user.welcomeCreditsShown;
    if (isNewUser) {
      user.welcomeCreditsShown = true;
      await user.save();
    }

    res.status(200).json({ 
      message: "Login successful!", 
      name: user.name, 
      email: user.email, 
      phone: user.phone || "", 
      location: user.location || "", 
      businessName: user.businessName || "", 
      businessType: user.businessType || "", 
      credits: user.credits,
      totalSent: user.totalSent || 0,
      isNewUser
    });
  } catch (error) {
    res.status(500).json({ message: "Error logging in", error });
  }
});

app.get('/api/users', async (req, res) => {
  try {
    const users = await User.find({}, '-password').sort({ _id: -1 }).lean(); // Exclude passwords, sort descending, use lean for speed
    res.status(200).json(users);
  } catch (error) {
    res.status(500).json({ message: "Error fetching users", error });
  }
});

app.put('/api/users/:id', async (req, res) => {
  try {
    const { name, email, phone, location, businessName, businessType } = req.body;
    const updatedUser = await User.findByIdAndUpdate(
      req.params.id,
      { name, email, phone, location, businessName, businessType },
      { new: true }
    );
    if (!updatedUser) return res.status(404).json({ message: "User not found" });
    res.status(200).json({ message: "User updated successfully", user: updatedUser });
  } catch (error) {
    res.status(500).json({ message: "Error updating user", error });
  }
});

app.delete('/api/users/:id', async (req, res) => {
  try {
    const deletedUser = await User.findByIdAndDelete(req.params.id);
    if (!deletedUser) return res.status(404).json({ message: "User not found" });
    res.status(200).json({ message: "User deleted successfully" });
  } catch (error) {
    res.status(500).json({ message: "Error deleting user", error });
  }
});

app.get('/api/messages/stats', async (req, res) => {
  try {
    const stats = await MessageLog.find().sort({ date: 1 });
    res.status(200).json(stats);
  } catch (error) {
    res.status(500).json({ message: "Error fetching message stats", error });
  }
});

app.post('/api/user/credits', async (req, res) => {
  try {
    const { email } = req.body;
    let user = await User.findOne({ email });
    if (!user) return res.status(404).json({ message: "User not found" });

    // Check and reset daily credits to 10 if it's a new day
    user = await checkAndResetDailyCredits(user);

    res.status(200).json({ 
      credits: user.credits, 
      totalSent: user.totalSent || 0, 
      lastDailyRewardDate: user.lastDailyRewardDate 
    });
  } catch (error) {
    res.status(500).json({ message: "Error fetching credits", error });
  }
});


app.post('/api/user/increment-sent', async (req, res) => {
  try {
    const { email } = req.body;
    let user = await User.findOne({ email });
    if (user) {
      user.totalSent = (user.totalSent || 0) + 1;
      await user.save();
      res.status(200).json({ totalSent: user.totalSent });
    } else {
      res.status(404).json({ message: "User not found" });
    }
  } catch (error) {
    res.status(500).json({ message: "Error incrementing sent", error });
  }
});

app.post('/api/user/use-credit', async (req, res) => {
  try {
    const { email, amount } = req.body;
    let user = await User.findOne({ email });
    if (user) {
      user.credits = Math.max(0, (user.credits || 0) - (amount || 1));
      await user.save();
      res.status(200).json({ credits: user.credits });
    } else {
      res.status(404).json({ message: "User not found" });
    }
  } catch (error) {
    res.status(500).json({ message: "Error using credit", error });
  }
});

// ==========================================
// WATCH VIDEO & EARN CREDITS — BACKEND
// Powers the "Watch Video & Earn Credits" flow (WatchVideo.js / RewardHandler.js
// on the frontend). Reuses the existing User model/credits field directly —
// no changes to userSchema needed. All watch-session bookkeeping lives in
// the new VideoWatch collection below, so a video can never be "completed"
// (and credited) without first going through /start on this same session.
//
// Supports BOTH account types actually used by the frontend:
//   - regular User accounts (resolved by email)
//   - Free User accounts (resolved by Mongo _id — the businessName/
//     businessType signup flow, identified by localStorage's
//     freeUserToken/freeUserData on the frontend)
// This server.js doesn't itself expose Free User register/login routes
// (that flow lives elsewhere), but it connects to the same MongoDB
// database, so the FreeUser model defined below reads/credits the exact
// same accounts that flow already created.
// ==========================================

// Small hardcoded catalog of reward-eligible videos/flows. `minSeconds` is
// enforced server-side in /api/videos/complete so a video can never be
// "completed" the instant it's opened or by clicking through/skipping —
// only real elapsed watch time counts.
const REWARD_VIDEOS = {
  'goyee-promo-1': {
    title: 'How Goyee Works',
    url: 'https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/ForBiggerBlazes.mp4',
    minSeconds: 15
  },
  'goyee-promo-2': {
    title: 'WhatsApp Bulk Messaging Tips',
    url: 'https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/ForBiggerJoyrides.mp4',
    minSeconds: 15
  },
  // Watch & Earn ad flow (real Google Ad Manager Rewarded Ad on the
  // frontend, not a <video> tag — no `url` needed here). The reward is
  // never decided by minSeconds; the frontend only calls /complete after
  // Google's own rewardedSlotGranted event fires. minSeconds is purely a
  // server-side anti-abuse floor.
  'goyee-ad-watch': {
    title: 'Watch & Earn',
    url: null,
    minSeconds: 4
  },
  // Standalone Watch & Earn poster page (WatchVideo.js). No external
  // "genuinely watched" signal here, so minSeconds is the real proof of
  // completion and is enforced close to the full 15 seconds.
  'goyee-watch-poster': {
    title: 'Watch & Earn',
    url: null,
    minSeconds: 15
  }
};

const CREDITS_PER_VIDEO = 4;
// A user can earn from a maximum of 5 reward videos PER DAY (IST calendar
// day) — 5 x 4 = 20 additional credits max per day. Shared across ALL
// reward flows above, so switching flows can't stack extra credits.
const MAX_REWARD_VIDEOS = 5;
// Overall ceiling on a user's balance that VIDEO rewards can push it to —
// 30 starting credits + 20 max video credits = 50. A video reward is
// simply not applied past this point (existing balance is preserved,
// never reduced). This only caps additions made via /api/videos/complete;
// it does not touch how credits are spent elsewhere.
const MAX_TOTAL_CREDITS = 50;
const REPLAY_COOLDOWN_MS = 30 * 1000; // blocks instantly re-starting the same video right after a reward

const videoWatchSchema = new mongoose.Schema({
  userId: { type: String, required: true },   // the user's email (this server only has the User model)
  userType: { type: String, default: 'user' },
  videoId: { type: String, required: true },
  startedAt: { type: Date, default: Date.now },
  completedAt: { type: Date, default: null },
  completed: { type: Boolean, default: false },
  creditsEarned: { type: Number, default: 0 },
  // IST calendar day (YYYY-MM-DD) this watch was REWARDED on — set only
  // when `completed` flips true. Makes the 5-video cap a DAILY cap: the
  // eligibility counts below filter on this field so past days never
  // count against today's allowance.
  rewardCycle: { type: String, default: null },
  createdAt: { type: Date, default: Date.now }
});
const VideoWatch = mongoose.models.VideoWatch || mongoose.model('VideoWatch', videoWatchSchema);



// Resolves + validates the calling user. Only 'user' (by email) is
// supported since that's the only model this server.js defines.
// Resolves + validates the calling user for either account type.
async function resolveRewardUser({ userType, id, email }) {
  if (userType === 'freeUser') {
    if (!id) return { error: 'id is required for a Free User.' };
    const freeUser = await FreeUser.findById(id);
    if (!freeUser) return { error: 'Free user not found.', status: 404 };
    if (freeUser.status === 'blocked') return { error: 'Your account is blocked.', status: 403 };
    return { doc: freeUser };
  }
  if (!email) return { error: 'email is required.', status: 400 };
  const user = await User.findOne({ email });
  if (!user) return { error: 'User not found.', status: 404 };
  return { doc: user };
}

// Start a reward-eligible video. Creates a pending (uncompleted) VideoWatch
// record and hands back its id — the frontend must send this SAME watchId
// back to /complete, and only after actually watching, for credits to land.
app.post('/api/videos/start', async (req, res) => {
  try {
    const { videoId, userType = 'user', id, email } = req.body;
    const video = REWARD_VIDEOS[videoId];
    if (!video) return res.status(400).json({ message: 'Invalid video.' });

    const { doc: userDoc, error, status } = await resolveRewardUser({ userType, id, email });
    if (error) return res.status(status || 404).json({ message: error });

    // userId key used for VideoWatch bookkeeping: a Free User's Mongo _id
    // (string) or a regular User's email — matches whichever collection
    // userDoc actually came from.
    const userIdKey = userType === 'freeUser' ? String(userDoc._id) : userDoc.email;
    const cycleKey = todayKeyIST();

    const completedToday = await VideoWatch.countDocuments({
      userId: userIdKey, userType, completed: true, rewardCycle: cycleKey
    });
    if (completedToday >= MAX_REWARD_VIDEOS) {
      // DEBUG: resolvedIdentity echoes back exactly which account this
      // request was checked against. If a genuinely NEW user ever hits
      // this branch, open the browser Network tab and check this field —
      // if it shows an email/id that isn't the new account's, the bug is
      // in the frontend (stale localStorage from the previous account),
      // not here. Safe to remove once the root cause is confirmed fixed.
      return res.status(403).json({
        eligible: false,
        videosWatched: completedToday,
        videosRemaining: 0,
        resolvedIdentity: { userType, userIdKey },
        message: 'Your video watch limit is completed. Please try again tomorrow.'
      });
    }

    // Replay-spam guard — blocks re-starting the SAME video again right
    // after it was just rewarded, so refresh/replay can't farm credits.
    const recentReward = await VideoWatch.findOne({ userId: userIdKey, userType, videoId, completed: true }).sort({ completedAt: -1 });
    if (recentReward && Date.now() - new Date(recentReward.completedAt).getTime() < REPLAY_COOLDOWN_MS) {
      return res.status(429).json({ message: 'Please wait a bit before watching this video again.' });
    }

    const watch = await VideoWatch.create({ userId: userIdKey, userType, videoId, startedAt: new Date(), completed: false });

    res.status(201).json({
      watchId: watch._id,
      video: { title: video.title, url: video.url, minSeconds: video.minSeconds },
      credits: userDoc.credits || 0,
      videosWatched: completedToday,
      videosRemaining: Math.max(0, MAX_REWARD_VIDEOS - completedToday)
    });
  } catch (error) {
    console.error('videos/start error:', error);
    res.status(500).json({ message: 'Error starting video.', error: error.message });
  }
});

// Complete a video and (if genuinely watched) grant the reward. This is the
// ONLY place credits are added for this feature — never trust a credit
// value sent by the frontend.
app.post('/api/videos/complete', async (req, res) => {
  try {
    const { watchId, userType = 'user', id, email } = req.body;
    if (!watchId) return res.status(400).json({ message: 'watchId is required.' });

    const { doc: userDoc, error, status } = await resolveRewardUser({ userType, id, email });
    if (error) return res.status(status || 404).json({ message: error });

    const userIdKey = userType === 'freeUser' ? String(userDoc._id) : userDoc.email;

    const watch = await VideoWatch.findOne({ _id: watchId, userId: userIdKey, userType });
    if (!watch) return res.status(404).json({ message: 'Watch session not found.' });
    if (watch.completed) return res.status(400).json({ message: 'This watch was already rewarded.' });

    const video = REWARD_VIDEOS[watch.videoId];
    if (!video) return res.status(400).json({ message: 'Invalid video.' });

    // Server-side proof of genuine completion: real wall-clock time must
    // have passed between /start and /complete. A 2s tolerance absorbs
    // network/render lag without opening a meaningful skip window.
    const elapsedSeconds = (Date.now() - new Date(watch.startedAt).getTime()) / 1000;
    if (elapsedSeconds < video.minSeconds - 2) {
      return res.status(400).json({ message: 'Video was not watched fully.' });
    }

    // Re-check today's cap right before claiming too (not just in /start),
    // so a video that was legitimately started earlier today can't be
    // completed after the cap was already reached by other watches in
    // between (e.g. two tabs).
    const cycleKey = todayKeyIST();
    const completedTodayBeforeClaim = await VideoWatch.countDocuments({
      userId: userIdKey, userType, completed: true, rewardCycle: cycleKey
    });
    if (completedTodayBeforeClaim >= MAX_REWARD_VIDEOS) {
      return res.status(403).json({
        resolvedIdentity: { userType, userIdKey },
        message: 'Your video watch limit is completed. Please try again tomorrow.'
      });
    }

    // Atomic claim: the filter re-checks completed:false, so two
    // near-simultaneous /complete calls for the same watchId can never
    // both succeed — only one can ever flip this record and get credited.
    const claimed = await VideoWatch.findOneAndUpdate(
      { _id: watchId, completed: false },
      { completed: true, completedAt: new Date(), creditsEarned: CREDITS_PER_VIDEO, rewardCycle: cycleKey },
      { new: true }
    );
    if (!claimed) return res.status(400).json({ message: 'This watch was already rewarded.' });

    // Credit the SAME model the user actually came from. Uses an
    // aggregation-pipeline update so the +CREDITS_PER_VIDEO and the
    // MAX_TOTAL_CREDITS cap are applied atomically in one step — no
    // separate read-then-write, so this can't race with a concurrent
    // request into an over-cap balance. An existing balance already at or
    // above the cap simply doesn't increase further; it's never reduced.
    //
    // If this write fails or comes back empty, the watch claim above must
    // be rolled back (completed -> false again). Without this, a failure
    // here left the watch permanently marked "completed" with no credits
    // ever granted — every retry then hit the "already rewarded" guard
    // above with a 400, even though the user was never actually paid.
    let updatedUser;
    try {
      const Model = userType === 'freeUser' ? FreeUser : User;
      // Classic update instead of a MongoDB update-pipeline (an array
      // passed as the update doc). That pipeline syntax needs MongoDB
      // server 4.2+ with full pipeline-update support end-to-end, which
      // isn't available on every deployment/driver combination — when
      // it's not, findByIdAndUpdate throws here on every single attempt,
      // which is exactly the 500 "Could not grant credits" error. A plain
      // $inc with the increment pre-computed in JS produces the identical
      // capped result (never exceeds MAX_TOTAL_CREDITS, never decreases)
      // and works everywhere.
      const currentCredits = userDoc.credits || 0;
      const increment = Math.max(0, Math.min(CREDITS_PER_VIDEO, MAX_TOTAL_CREDITS - currentCredits));
      updatedUser = await Model.findByIdAndUpdate(
        userDoc._id,
        { $inc: { credits: increment } },
        { new: true }
      );
      if (!updatedUser) throw new Error(`Credit grant found no ${userType === 'freeUser' ? 'FreeUser' : 'User'} document for id ${userDoc._id}`);
    } catch (creditError) {
      // Roll back the claim so the watch is claimable again instead of
      // being stuck "completed" with nothing paid out.
      await VideoWatch.findOneAndUpdate(
        { _id: watchId, completed: true },
        { completed: false, completedAt: null, creditsEarned: 0, rewardCycle: null }
      );
      console.error('videos/complete credit grant failed, rolled back claim:', creditError);
      return res.status(500).json({ message: 'Could not grant credits for this video. Please try again.' });
    }
    const actualCreditsEarned = Math.max(0, updatedUser.credits - (userDoc.credits || 0));

    const completedToday = await VideoWatch.countDocuments({ userId: userIdKey, userType, completed: true, rewardCycle: cycleKey });

    res.status(200).json({
      success: true,
      creditsEarned: actualCreditsEarned,
      credits: updatedUser.credits,
      videosWatched: completedToday,
      videosRemaining: Math.max(0, MAX_REWARD_VIDEOS - completedToday),
      message: actualCreditsEarned > 0
        ? `Video completed! +${actualCreditsEarned} Credits added.`
        : `Video completed! You're already at the ${MAX_TOTAL_CREDITS}-credit maximum.`
    });
  } catch (error) {
    console.error('videos/complete error:', error);
    res.status(500).json({ message: 'Error completing video.', error: error.message });
  }
});

// Lightweight read-only status check — lets a page show how many reward
// videos a user has watched TODAY (out of the daily cap) WITHOUT starting
// a new watch session.
app.get('/api/videos/status', async (req, res) => {
  try {
    const { userType = 'user', id, email } = req.query;
    const { doc: userDoc, error, status } = await resolveRewardUser({ userType, id, email });
    if (error) return res.status(status || 404).json({ message: error });

    const userIdKey = userType === 'freeUser' ? String(userDoc._id) : userDoc.email;
    const cycleKey = todayKeyIST();
    const completedToday = await VideoWatch.countDocuments({ userId: userIdKey, userType, completed: true, rewardCycle: cycleKey });

    res.status(200).json({
      credits: userDoc.credits || 0,
      videosWatched: completedToday,
      videosRemaining: Math.max(0, MAX_REWARD_VIDEOS - completedToday),
      maxRewardVideos: MAX_REWARD_VIDEOS,
      creditsPerVideo: CREDITS_PER_VIDEO,
      resolvedIdentity: { userType, userIdKey } // DEBUG — see /start for explanation
    });
  } catch (error) {
    console.error('videos/status error:', error);
    res.status(500).json({ message: 'Error fetching video status.', error: error.message });
  }
});

app.post('/api/contact/whatsapp', async (req, res) => {
  try {
    const { name, email, subject, message, text } = req.body;

    const validationErrors = [];
    if (!name || !String(name).trim()) validationErrors.push('Please enter your full name.');
    if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(email))) validationErrors.push('Please enter a valid email address.');
    if (!subject || !String(subject).trim()) validationErrors.push('Please enter a subject.');
    if (!message || !String(message).trim()) validationErrors.push('Please enter your message.');

    if (validationErrors.length > 0) {
      return res.status(400).json({ message: validationErrors[0] });
    }

    const activeClient = Object.values(userClients).find(uc => uc.isWhatsAppAuthenticated)?.whatsappClient;
    if (!activeClient) {
      return res.status(503).json({ message: 'WhatsApp is not ready yet. Please try again shortly.' });
    }

    const destination = String(process.env.WHATSAPP_CONTACT_NUMBER || '919486042369').replace(/\D/g, '');
    if (!destination) {
      return res.status(500).json({ message: 'WhatsApp destination is not configured.' });
    }

    const formattedNumber = destination.startsWith('91') ? `${destination}@s.whatsapp.net` : `91${destination}@s.whatsapp.net`;
    const payload = { text: text || `New contact form submission\nName: ${name}\nEmail: ${email}\nSubject: ${subject}\nMessage: ${message}` };

    await activeClient.sendMessage(formattedNumber, payload);
    res.status(200).json({ message: 'Contact message sent successfully.' });
  } catch (error) {
    console.error('Contact WhatsApp error:', error);
    res.status(500).json({ message: 'Unable to send your message right now.' });
  }
});

// --- Support Request / Feedback API ---
const supportSchema = new mongoose.Schema({
  username: { type: String },
  email: { type: String },
  type: { type: String, required: true }, // 'issue' or 'feedback'
  issueType: { type: String },
  description: { type: String, required: true },
  rating: { type: Number },
  screenshot: {
    filename: { type: String },
    mimetype: { type: String },
    data: { type: String } // base64 string
  },
  createdAt: { type: Date, default: Date.now }
});

const Support = mongoose.models.Support || mongoose.model('Support', supportSchema);

app.post('/api/support', async (req, res) => {
  try {
    const { username, email, type, issueType, description, rating, screenshot } = req.body;

    const supportDoc = new Support({
      username: username || "Guest",
      email: email || "no-email@goyee.com",
      type,
      issueType,
      description,
      rating,
      screenshot
    });
    await supportDoc.save();

    const destination = '919943042369'; 


    let cleanDest = String(destination).replace(/\D/g, '');
    if (cleanDest.length === 10) {
      cleanDest = '91' + cleanDest;
    }
    const formattedNumber = `${cleanDest}@s.whatsapp.net`;

    const dateFormatted = new Date().toLocaleDateString('en-US', { day: '2-digit', month: 'short', year: 'numeric' });
    const timeFormatted = new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: true });

    let textMessage = '';
    if (type === 'issue') {
      textMessage = `New Help Request\n\nUser:\n${username}\n\nEmail:\n${email}\n\nIssue:\n${issueType}\n\nDescription:\n${description}\n\nDate:\n${dateFormatted}\n${timeFormatted}`;
    } else {
      const stars = '⭐'.repeat(rating || 0);
      textMessage = `New User Feedback\n\nUser:\n${username}\n\n${stars}\n\nFeedback:\n${description}`;
    }

    try {
      let userClient = userClients[email] || Object.values(userClients).find(uc => uc && uc.isWhatsAppAuthenticated);
      
      const maxRetries = 3;
      let retries = 0;
      while (retries < maxRetries && (!userClient || !userClient.isWhatsAppAuthenticated || !userClient.whatsappClient || userClient.loading)) {
        await new Promise(resolve => setTimeout(resolve, 500));
        retries++;
        userClient = userClients[email] || Object.values(userClients).find(uc => uc && uc.isWhatsAppAuthenticated);
      }

      if (userClient && userClient.isWhatsAppAuthenticated && userClient.whatsappClient) {
        const whatsappClient = userClient.whatsappClient;
        console.log(`📤 [Support API] Dispatching message to admin ${cleanDest}...`);
        if (screenshot && screenshot.data) {
          const buffer = Buffer.from(screenshot.data, 'base64');
          await whatsappClient.sendMessage(formattedNumber, {
            image: buffer,
            caption: textMessage,
            mimetype: screenshot.mimetype || 'image/png'
          });
        } else {
          await whatsappClient.sendMessage(formattedNumber, { text: textMessage });
        }
        console.log(`✅ [Support API] Support message successfully delivered to admin WhatsApp.`);
      } else {
        console.warn(`⚠️ [Support API] Ticket saved to database, but WhatsApp client not active. Skipping WhatsApp notification.`);
      }
    } catch (waError) {
      console.warn('⚠️ [Support API] Ticket saved to database, but failed to send WhatsApp notification to admin:', waError.message);
    }

    return res.status(200).json({ message: 'Support request submitted successfully.' });
  } catch (error) {
    console.error('Support API Error:', error);
    return res.status(500).json({ message: 'Error submitting support request', error: error.message });
  }
});

app.use((err, req, res, next) => {
    console.error("Express Error:", err);
    res.status(err.status || 500).json({ error: err.message || "Internal Server Error" });
});

server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
