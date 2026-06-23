const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.static(path.join(__dirname, 'public')));

// ============ 匹配池 ============
const waitingPool = []; // { ws, id, tags, name }
const connections = new Map(); // ws -> userInfo

// 敏感信息正则
const SENSITIVE_PATTERNS = [
  { pattern: /1[3-9]\d{9}/g, label: '手机号' },
  { pattern: /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g, label: '邮箱' },
  { pattern: /(?:微信|wx|vx|VX|weixin)[\s:：]*[a-zA-Z0-9_-]{5,}/gi, label: '微信号' },
  { pattern: /(?:QQ|qq)[\s:：]*\d{5,}/gi, label: 'QQ号' },
];

function filterSensitive(text) {
  let filtered = text;
  let blocked = false;
  for (const { pattern, label } of SENSITIVE_PATTERNS) {
    if (pattern.test(text)) {
      filtered = filtered.replace(pattern, `[${label}已拦截]`);
      blocked = true;
    }
  }
  return { filtered, blocked };
}

// ============ 随机匹配 ============
function tryMatch() {
  while (waitingPool.length >= 2) {
    // 打乱取两个
    const shuffled = [...waitingPool];
    for (let i = shuffled.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
    }
    const user1 = shuffled[0];
    const user2 = shuffled[1];

    // 从池中移除
    const i1 = waitingPool.indexOf(user1);
    const i2 = waitingPool.indexOf(user2);
    waitingPool.splice(Math.max(i1, i2), 1);
    waitingPool.splice(Math.min(i1, i2), 1);

    // 更新连接状态
    connections.get(user1.ws).partner = user2.ws;
    connections.get(user1.ws).state = 'chatting';
    connections.get(user2.ws).partner = user1.ws;
    connections.get(user2.ws).state = 'chatting';

    // 共同标签
    const commonTags = user1.tags.filter(t => user2.tags.includes(t));

    // 通知双方
    const msg1 = JSON.stringify({
      type: 'match_found',
      partner: {
        id: user2.id,
        name: user2.name,
        tags: user2.tags
      },
      commonTags
    });
    const msg2 = JSON.stringify({
      type: 'match_found',
      partner: {
        id: user1.id,
        name: user1.name,
        tags: user1.tags
      },
      commonTags
    });

    sendSafe(user1.ws, msg1);
    sendSafe(user2.ws, msg2);

    console.log(`匹配: ${user1.name}(${user1.id}) <-> ${user2.name}(${user2.id})`);
  }
}

function sendSafe(ws, data) {
  try {
    if (ws.readyState === 1) ws.send(data);
  } catch (e) {}
}

function removeFromPool(ws) {
  const idx = waitingPool.findIndex(u => u.ws === ws);
  if (idx !== -1) waitingPool.splice(idx, 1);
}

function handleDisconnect(ws) {
  const info = connections.get(ws);
  if (!info) return;

  removeFromPool(ws);

  if (info.partner) {
    sendSafe(info.partner, JSON.stringify({
      type: 'partner_left',
      reason: '对方已离开'
    }));
    const pInfo = connections.get(info.partner);
    if (pInfo) {
      pInfo.partner = null;
      pInfo.state = 'idle';
    }
  }

  connections.delete(ws);
  console.log(`断开: ${info.name}(${info.id})`);
}

// ============ WebSocket ============
let idCounter = 1;

wss.on('connection', (ws) => {
  const userId = String(idCounter++).padStart(4, '0');
  const userName = `匿名用户 #${userId}`;
  const userInfo = {
    id: userId,
    name: userName,
    state: 'idle',
    partner: null,
    tags: []
  };
  connections.set(ws, userInfo);

  console.log(`连接: ${userName}`);

  // 发送身份信息
  ws.send(JSON.stringify({
    type: 'identity',
    id: userId,
    name: userName
  }));

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); }
    catch (e) { return; }

    const info = connections.get(ws);
    if (!info) return;

    switch (msg.type) {
      case 'join_match': {
        info.tags = msg.tags || [];
        info.state = 'waiting';
        // 先清理旧记录
        removeFromPool(ws);
        waitingPool.push({
          ws,
          id: info.id,
          name: info.name,
          tags: info.tags
        });
        console.log(`加入匹配池: ${info.name}, 标签: ${info.tags.join(',')}, 池大小: ${waitingPool.length}`);
        ws.send(JSON.stringify({ type: 'waiting', poolSize: waitingPool.length }));
        tryMatch();
        break;
      }

      case 'cancel_match': {
        removeFromPool(ws);
        info.state = 'idle';
        ws.send(JSON.stringify({ type: 'match_cancelled' }));
        break;
      }

      case 'send_message': {
        if (!info.partner) return;
        const { filtered, blocked } = filterSensitive(msg.text || '');

        // 发给对方
        sendSafe(info.partner, JSON.stringify({
          type: 'new_message',
          text: filtered,
          fromMe: false,
          from: info.name
        }));

        // 回显给自己
        ws.send(JSON.stringify({
          type: 'new_message',
          text: msg.text,
          fromMe: true
        }));

        // 如果触发了拦截，给对方发警告，给自己发提示
        if (blocked) {
          sendSafe(info.partner, JSON.stringify({
            type: 'system_warning',
            text: '对方尝试发送联系方式，已被系统拦截'
          }));
          ws.send(JSON.stringify({
            type: 'system_notice',
            text: '你发送的消息包含敏感信息，已被过滤'
          }));
        }
        break;
      }

      case 'end_chat': {
        if (info.partner) {
          sendSafe(info.partner, JSON.stringify({
            type: 'partner_left',
            reason: '对方结束了对话'
          }));
          const pInfo = connections.get(info.partner);
          if (pInfo) {
            pInfo.partner = null;
            pInfo.state = 'idle';
          }
          info.partner = null;
        }
        info.state = 'idle';
        ws.send(JSON.stringify({ type: 'chat_ended' }));
        break;
      }

      case 'ping': {
        ws.send(JSON.stringify({ type: 'pong' }));
        break;
      }
    }
  });

  ws.on('close', () => handleDisconnect(ws));
  ws.on('error', () => handleDisconnect(ws));
});

// ============ 启动 ============
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n🎭 WhisperChat 已启动`);
  console.log(`   地址: http://localhost:${PORT}\n`);
});
