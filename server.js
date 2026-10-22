const http=require('http'),fs=require('fs'),path=require('path'),{WebSocketServer}=require('ws');
const page=path.join(__dirname,'public','index.html');
const srv=http.createServer((q,s)=>fs.readFile(page,(e,d)=>{s.writeHead(e?500:200,{'Content-Type':'text/html; charset=utf-8'});s.end(d||'error')}));
const wss=new WebSocketServer({server:srv,maxPayload:65536}),rooms=new Map();
const id=()=>Math.random().toString(36).slice(2,10);
wss.on('connection',ws=>{
  let rm=null;const me=id();
  const send=(w,o)=>w.readyState===1&&w.send(JSON.stringify(o));
  const cast=(r,o)=>r.forEach(c=>send(c.ws,o));
  const plist=r=>[...r.values()].map(c=>({peer:c.peer,presence:c.pres}));
  ws.on('message',raw=>{
    let m;try{m=JSON.parse(raw)}catch{return}
    if(m.t==='join'&&!rm){
      const code=String(m.room).replace(/[^a-z0-9-]/gi,'').slice(0,24);if(!code)return;
      if(!rooms.has(code))rooms.set(code,new Map());
      const r=rooms.get(code);if(r.size>=12)return ws.close();
      rm=r;r.set(me,{ws,peer:me,pres:{}});send(ws,{t:'welcome',peer:me});cast(r,{t:'peers',list:plist(r)});return}
    if(!rm)return;
    if(m.t==='emit')cast(rm,{t:'msg',topic:String(m.topic).slice(0,30),data:m.data,peer:me});
    if(m.t==='pres'&&m.patch&&typeof m.patch==='object'&&JSON.stringify(m.patch).length<600){Object.assign(rm.get(me).pres,m.patch);cast(rm,{t:'peers',list:plist(rm)})}
  });
  ws.on('close',()=>{if(!rm)return;rm.delete(me);if(rm.size)cast(rm,{t:'peers',list:plist(rm)});else for(const[k,v]of rooms)if(v===rm)rooms.delete(k)});
});
srv.listen(process.env.PORT||3000,()=>console.log('Party Palace running on port '+(process.env.PORT||3000)));
