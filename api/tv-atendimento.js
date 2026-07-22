// api/tv-atendimento.js — espelho de /api/atendimento para o sistema TV
const U=(process.env.UPSTASH_URL||'').replace(/['"]/g,'').trim();
const T=(process.env.UPSTASH_TOKEN||'').replace(/[\n\r'"]/g,'').trim();

async function dbGet(key){
  try{
    const r=await fetch(`${U}/get/${key}`,{headers:{Authorization:`Bearer ${T}`}});
    const j=await r.json();
    return j.result?JSON.parse(j.result):null;
  }catch{return null;}
}

function brtStartOf(unit){
  const now=new Date();
  const fmt=new Intl.DateTimeFormat('en-CA',{timeZone:'America/Sao_Paulo',year:'numeric',month:'2-digit',day:'2-digit'});
  const [y,m,d]=fmt.format(now).split('-').map(Number);
  if(unit==='day')  return new Date(Date.UTC(y,m-1,d,3,0,0,0));
  if(unit==='week'){
    const brtDate=new Date(now.toLocaleString('en-US',{timeZone:'America/Sao_Paulo'}));
    const dow=brtDate.getDay();const daysToMon=dow===0?6:dow-1;
    return new Date(Date.UTC(y,m-1,d-daysToMon,3,0,0,0));
  }
  if(unit==='month') return new Date(Date.UTC(y,m-1,1,3,0,0,0));
  return new Date(0);
}

export default async function handler(req,res){
  // 🔐 TF-AUTH (Fase 1): chave obrigatória em toda chamada
  const _tfk = (req.query && req.query.k) || req.headers['x-tf-key'] || '';
  if (_tfk !== ((process.env.TECHFLOW_KEY || 'tfk-re2026-Bx7mQp9zKw4Y').trim())) {
    return res.status(401).json({ ok: false, error: 'não autorizado' });
  }

  res.setHeader('Access-Control-Allow-Origin','https://reparoeletroadm.com');
  res.setHeader('Cache-Control','no-cache');
  if(req.method==='OPTIONS') return res.status(200).end();

  if(req.query.action==='metrics'){
    try{
      const todayUTC=brtStartOf('day'),weekUTC=brtStartOf('week'),monthUTC=brtStartOf('month');

      const [fichasDb,boardDb,orcDb,finDb,compDb]=await Promise.all([
        dbGet('fichas_tv'),
        dbGet('tv_board'),
        dbGet('tv_orcamentos'),
        dbGet('tv_financeiro'),
        dbGet('tv_compras_pecas'),
      ]);

      // ── FICHAS TV (planilha) ──────────────────────────────────────────────
      const fichasArr=(fichasDb?.fichas||[]);
      const ficDateOf=f=>new Date(f.criadoEm||0).getTime();
      const fichasTotal=fichasArr.length;
      const ficHoje   =fichasArr.filter(f=>ficDateOf(f)>=todayUTC.getTime()).length||null;
      const ficSem    =fichasArr.filter(f=>ficDateOf(f)>=weekUTC.getTime()).length||null;
      const ficMes    =fichasArr.filter(f=>ficDateOf(f)>=monthUTC.getTime()).length||null;
      const fichasHojeList=fichasArr
        .filter(f=>ficDateOf(f)>=todayUTC.getTime())
        .map(f=>({id:f.id,title:(f.nome||'')+(f.equipamento?' — '+f.equipamento:''),createdAt:f.criadoEm}));

      // ── BOARD TV (aprovados, produção, finalizados) ───────────────────────
      const cards=(boardDb?.cards||[]);
      const cardTs=c=>new Date(c.criadoEm||c.movedAt||0).getTime();

      // Aprovados = cards que passaram por 'aprovado'
      const aprvCards=cards.filter(c=>(c.phaseId==='aprovado')||(c.history||[]).some(h=>h.phaseId==='aprovado'));
      const aprvTotal=aprvCards.length;
      const aprvTs   =aprvCards.map(c=>c.criadoEm||c.movedAt).filter(Boolean);
      const aprvHoje =aprvCards.filter(c=>cardTs(c)>=todayUTC.getTime()).length||null;
      const aprvSem  =aprvCards.filter(c=>cardTs(c)>=weekUTC.getTime()).length||null;

      // Cadastrados = todos os cards já no board
      const cadTotal=cards.length;
      const cadH    =cards.filter(c=>cardTs(c)>=todayUTC.getTime()).length||null;
      const cadS    =cards.filter(c=>cardTs(c)>=weekUTC.getTime()).length||null;
      const cadM    =cards.filter(c=>cardTs(c)>=monthUTC.getTime()).length||null;

      // Vendidos / Finalizados = loja_feito + delivery_feito
      const vendCards=cards.filter(c=>['loja_feito','delivery_feito'].includes(c.phaseId));
      const vendTotal=vendCards.length;
      const vendH   =vendCards.filter(c=>cardTs(c)>=todayUTC.getTime()).length||null;
      const vendS   =vendCards.filter(c=>cardTs(c)>=weekUTC.getTime()).length||null;

      // Comprados = compras de peças TV
      const compArr=(compDb?.fichas||[]);
      const comprados=compArr.filter(f=>f.status==='comprado');
      const compTs=f=>new Date(f.statusAt||f.criadoEm||0).getTime();
      const compTotal=comprados.length;
      const compH   =comprados.filter(f=>compTs(f)>=todayUTC.getTime()).length;
      const compS   =comprados.filter(f=>compTs(f)>=weekUTC.getTime()).length;
      const compMArr=comprados.filter(f=>compTs(f)>=monthUTC.getTime());
      const compMCount=compMArr.length;

      // ── ORÇAMENTOS TV ───────────────────────────────────────────────────
      const orcs=(orcDb?.fichas||orcDb?.cards||[]);
      const orcTs=orcs.map(o=>o.criadoEm||o.enviadoEm||o.createdAt).filter(Boolean);
      const orcHoje=orcTs.filter(ts=>new Date(ts).getTime()>=todayUTC.getTime()).length||null;
      const orcSem =orcTs.filter(ts=>new Date(ts).getTime()>=weekUTC.getTime()).length||null;

      // ── PAGAMENTO TV ───────────────────────────────────────────────────
      const fins=(finDb?.records||finDb?.fichas||[]);
      const pgTs=fins.map(r=>{
        const hh=(r.history||[]).find(e=>e.phaseId==='pagamento_confirmado');
        return hh?.ts||hh?.movedAt||null;
      }).filter(Boolean);
      const pgHoje=pgTs.filter(ts=>new Date(ts).getTime()>=todayUTC.getTime()).length||null;
      const pgSem =pgTs.filter(ts=>new Date(ts).getTime()>=weekUTC.getTime()).length||null;

      // ── Monthly card (mesmo formato do ADM) ───────────────────────────
      const pendentes=cadTotal-vendTotal;
      const backlog=Math.max(0,pendentes-10);
      const fichasEsteMes=ficMes||0;
      const fichasAnteriores=fichasTotal-(ficMes||0);
      const cadastradas=cadM||0;
      const compAnteriores=comprados.filter(f=>compTs(f)<monthUTC.getTime()).length;

      const monthly={
        comprados:compMCount,cadastrados:cadM||0,
        falta:pendentes,backlog,compAnteriores,
        fichasEsteMes,fichasAnteriores,cadastradas,pendentes,
      };

      // ERP placeholder (TV não usa Pipefy direto)
      const erp={total:0,semValor:0,hoje:null,semana:null};

      const m={
        fichas:    {total:fichasTotal,hoje:ficHoje,semana:ficSem,mes:ficMes},
        comprados: {total:compTotal,hoje:compH,semana:compS,mes:compMCount},
        cadastrados:{total:cadTotal,hoje:cadH,semana:cadS,mes:cadM},
        vendidos:  {total:vendTotal,hoje:vendH,semana:vendS},
        disponiveis:cadTotal-vendTotal,
        erp,
        orcamento: {hoje:orcHoje,semana:orcSem,timestamps:orcTs},
        pagamento: {hoje:pgHoje,semana:pgSem,timestamps:pgTs},
        aprovados: {hoje:aprvHoje,semana:aprvSem,total:aprvTotal,timestamps:aprvTs},
        monthly,
        fichasHojeList,
        updatedAt:new Date().toISOString(),
      };

      return res.status(200).json({ok:true,...m});
    }catch(e){
      return res.status(500).json({ok:false,error:e.message});
    }
  }

  return res.status(404).json({ok:false,error:'Ação não encontrada'});
}
