
// RevSight AI - Dual-source resilient data loader
// Exposes: window.revoptDataPromise -> Promise<rows[]>
(function(){
  const MANIFEST_URL = "data/manifest.json";
  const SCHEMA_URL   = "data/schema_v1.json";
  const LKG_KEY      = "revopt:lkg:data";
  const LKG_META_KEY = "revopt:lkg:meta";

  async function sha256(text) {
    const enc = new TextEncoder().encode(text);
    const buf = await crypto.subtle.digest('SHA-256', enc);
    return Array.from(new Uint8Array(buf)).map(b=>b.toString(16).padStart(2,'0')).join('');
  }

  function saveLKG(meta, rows){
    try{
      localStorage.setItem(LKG_KEY, JSON.stringify(rows));
      localStorage.setItem(LKG_META_KEY, JSON.stringify(meta));
    }catch(e){ /* storage full or disabled */ }
  }
  function loadLKG(){
    try{
      const rows = JSON.parse(localStorage.getItem(LKG_KEY) || "null");
      const meta = JSON.parse(localStorage.getItem(LKG_META_KEY) || "null");
      if(!rows || !meta) return null;
      return { rows, meta };
    }catch(e){ return null; }
  }

  function validateSchema(rows, schema){
    if(!rows || !rows.length) throw new Error("empty dataset");
    const cols = Object.keys(rows[0] || {});
    const missing = (schema.required_columns || []).filter(c => !cols.includes(c));
    if(missing.length) throw new Error("missing columns: " + missing.join(", "));
    // numeric warn only
    const numCols = schema.numeric_columns || [];
    for(const col of numCols){
      for(let i=0;i<Math.min(rows.length,50);i++){
        const v = rows[i][col];
        if(v!==null && v!==undefined && v!==""){
          const vv = String(v).replace(/[$,%]/g,"").replace(/,/g,"");
          if(Number.isNaN(Number(vv))){
            console.warn("[revopt] Non-numeric sample for", col, "→", v);
            break;
          }
        }
      }
    }
  }

  async function fetchText(url, version){
    const final = version ? `${url}?v=${encodeURIComponent(version)}` : url;
    const res = await fetch(final, { cache: "reload" });
    if(!res.ok) throw new Error(`HTTP ${res.status} for ${final}`);
    return await res.text();
  }

  function parseCSV(text){
    return new Promise((resolve,reject)=>{
      if(!window.Papa){ return reject(new Error("PapaParse not loaded")); }
      Papa.parse(text, {
        header:true,
        dynamicTyping:false,
        skipEmptyLines:"greedy",
        transformHeader: h => (h || "").trim(),
        complete: (r)=> resolve(r.data),
        error: reject
      });
    });
  }

  async function loadNetwork(){
    const manifestText = await fetchText(MANIFEST_URL);
    const manifest = JSON.parse(manifestText);
    const schemaText = await fetchText(SCHEMA_URL, manifest.schema_version || "");
    const schema = JSON.parse(schemaText);
    const csvText = await fetchText(manifest.url, manifest.version);
    const hash = await sha256(csvText);
    if(manifest.sha256 && manifest.sha256.toLowerCase() !== hash.toLowerCase()){
      throw new Error(`Checksum mismatch. Expected ${manifest.sha256} got ${hash}`);
    }
    const rows = await parseCSV(csvText);
    validateSchema(rows, schema);
    saveLKG({ source:"network", manifest }, rows);
    return { rows, meta: { source:"network", manifest } };
  }

  async function loadData(){
    try{
      const net = await loadNetwork();
      return net;
    }catch(e){
      console.warn("[revopt] network path failed:", e.message);
      const lkg = loadLKG();
      if(lkg){ return lkg; }
      throw new Error("No data available (network failed and no LKG). " + e.message);
    }
  }

  // public: a single promise for app code
  window.revoptDataPromise = (async ()=>{
    const { rows, meta } = await loadData();
    // also expose a sync snapshot after promise resolves
    window.revoptData = rows;
    window.revoptDataMeta = meta;
    return rows;
  })();
})();
