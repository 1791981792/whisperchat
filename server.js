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
const idToWs = new Map(); // userId -> ws (for friend online lookup)

// ============ 好友系统 ============
const friends = new Map(); // userId -> Set<friendId>
const friendRequests = []; // [{id, fromUserId, fromName, fromWs, toUserId, toWs, status}]
const friendMessages = new Map(); // "uid1_uid2" -> [{from, text, time}]
let reqCounter = 1;

function friendKey(a, b) { return a < b ? `${a}_${b}` : `${b}_${a}`; }

function addFriendPair(a, b) {
  if (!friends.has(a)) friends.set(a, new Set());
  if (!friends.has(b)) friends.set(b, new Set());
  friends.get(a).add(b);
  friends.get(b).add(a);
}

function getFriends(uid) {
  return friends.get(uid) ? [...friends.get(uid)] : [];
}

function getFriendListWithNames(uid) {
  return getFriends(uid).map(fid => {
    const key = friendKey(uid, fid);
    const msgs = friendMessages.get(key) || [];
    return {
      id: fid,
      name: `匿名用户 #${fid}`,
      online: idToWs.has(fid),
      lastMsg: msgs.length > 0 ? msgs[msgs.length - 1] : null
    };
  });
}

function broadcastFriendStatus(friendId, online) {
  for (const fid of getFriends(friendId)) {
    const ws = idToWs.get(fid);
    if (ws) {
      sendSafe(ws, JSON.stringify({
        type: 'friend_status',
        friendId,
        online
      }));
    }
  }
}

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
  broadcastPoolUpdate();
}

function sendSafe(ws, data) {
  try {
    if (ws.readyState === 1) ws.send(data);
  } catch (e) {}
}

function broadcastPoolUpdate() {
  const update = JSON.stringify({
    type: 'pool_update',
    poolSize: waitingPool.length
  });
  for (const user of waitingPool) {
    sendSafe(user.ws, update);
  }
}

function removeFromPool(ws) {
  const idx = waitingPool.findIndex(u => u.ws === ws);
  if (idx !== -1) {
    waitingPool.splice(idx, 1);
    broadcastPoolUpdate();
  }
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

  // 好友离线通知
  broadcastFriendStatus(info.id, false);
  idToWs.delete(info.id);
  connections.delete(ws);
  console.log(`断开: ${info.name}(${info.id})`);
}

// ============ WebSocket ============
let idCounter = 1;
const deviceUserMap = new Map(); // deviceId -> userId

wss.on('connection', (ws) => {
  let userId, userName, userInfo;
  let inited = false;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); }
    catch (e) { return; }

    // 首次消息必须是 init
    if (!inited) {
      if (msg.type !== 'init') return;

      const deviceId = msg.deviceId || 'anon_' + (idCounter++);
      if (deviceUserMap.has(deviceId)) {
        userId = deviceUserMap.get(deviceId);
      } else {
        userId = String(idCounter++).padStart(4, '0');
        deviceUserMap.set(deviceId, userId);
      }
      userName = `匿名用户 #${userId}`;
      userInfo = {
        id: userId,
        name: userName,
        state: 'idle',
        partner: null,
        tags: []
      };
      connections.set(ws, userInfo);
      idToWs.set(userId, ws);

      // 从客户端恢复好友关系
      if (msg.friends && msg.friends.length > 0) {
        for (const fid of msg.friends) {
          addFriendPair(userId, fid);
        }
      }

      // 同步聊天历史（合并去重）
      if (msg.chatHistory) {
        for (const [key, msgs] of Object.entries(msg.chatHistory)) {
          if (!friendMessages.has(key)) {
            friendMessages.set(key, msgs);
          } else {
            const existing = friendMessages.get(key);
            const seen = new Set(existing.map(m => `${m.from}|${m.time}`));
            for (const m of msgs) {
              if (!seen.has(`${m.from}|${m.time}`)) {
                existing.push(m);
                seen.add(`${m.from}|${m.time}`);
              }
            }
          }
        }
      }

      inited = true;
      console.log(`连接: ${userName} (device: ${deviceId.slice(0, 8)}...)`);

      const friendList = getFriendListWithNames(userId);
      ws.send(JSON.stringify({
        type: 'identity',
        id: userId,
        name: userName,
        friends: friendList
      }));
      broadcastFriendStatus(userId, true);
      return;
    }

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
        broadcastPoolUpdate();
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

      case 'friend_request': {
        // 向聊天对象发送好友申请
        if (!info.partner) return;
        const pInfo = connections.get(info.partner);
        if (!pInfo) return;
        
        const reqId = String(reqCounter++);
        const request = {
          id: reqId,
          fromUserId: info.id,
          fromName: info.name,
          fromWs: ws,
          toUserId: pInfo.id,
          toWs: info.partner,
          status: 'pending'
        };
        friendRequests.push(request);
        
        // 通知对方
        sendSafe(info.partner, JSON.stringify({
          type: 'friend_request',
          requestId: reqId,
          fromId: info.id,
          fromName: info.name
        }));
        // 通知自己
        ws.send(JSON.stringify({
          type: 'friend_request_sent',
          requestId: reqId
        }));
        break;
      }

      case 'friend_response': {
        const req = friendRequests.find(r => r.id === msg.requestId);
        if (!req || req.status !== 'pending') return;

        if (msg.accept) {
          addFriendPair(req.fromUserId, req.toUserId);
          req.status = 'accepted';

          // 通知双方
          sendSafe(req.fromWs, JSON.stringify({
            type: 'friend_added',
            friendId: req.toUserId,
            friendName: `匿名用户 #${req.toUserId}`
          }));
          sendSafe(req.toWs, JSON.stringify({
            type: 'friend_added',
            friendId: req.fromUserId,
            friendName: req.fromName
          }));

          // 通知双方对方在线状态
          sendSafe(req.fromWs, JSON.stringify({
            type: 'friend_status',
            friendId: req.toUserId,
            online: true
          }));
          sendSafe(req.toWs, JSON.stringify({
            type: 'friend_status',
            friendId: req.fromUserId,
            online: true
          }));
        } else {
          req.status = 'rejected';
          sendSafe(req.fromWs, JSON.stringify({
            type: 'friend_request_rejected',
            requestId: req.id
          }));
        }
        break;
      }

      case 'friend_message': {
        const toWs = idToWs.get(msg.toId);
        if (!toWs) {
          ws.send(JSON.stringify({ type: 'friend_msg_error', text: '对方不在线' }));
          return;
        }

        const key = friendKey(info.id, msg.toId);
        if (!friendMessages.has(key)) friendMessages.set(key, []);
        const record = { from: info.id, text: msg.text, time: Date.now() };
        friendMessages.get(key).push(record);

        // 发给对方
        sendSafe(toWs, JSON.stringify({
          type: 'friend_message',
          fromId: info.id,
          fromName: info.name,
          text: msg.text,
          time: record.time
        }));
        // 回显
        ws.send(JSON.stringify({
          type: 'friend_message',
          fromId: info.id,
          fromName: info.name,
          text: msg.text,
          fromMe: true,
          time: record.time
        }));
        break;
      }

      case 'get_friend_list': {
        ws.send(JSON.stringify({
          type: 'friend_list',
          friends: getFriendListWithNames(info.id)
        }));
        break;
      }

      case 'get_friend_history': {
        const key = friendKey(info.id, msg.friendId);
        const msgs = friendMessages.get(key) || [];
        msgs.forEach(m => {
          sendSafe(ws, JSON.stringify({
            type: 'friend_message',
            fromId: m.from,
            fromName: `匿名用户 #${m.from}`,
            text: m.text,
            fromMe: m.from === info.id,
            time: m.time
          }));
        });
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
