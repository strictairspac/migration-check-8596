import {batches,mappings,runs,acceptances} from '../api/business.js';
export const store={batches:[],mappings:[],runs:[],acceptances:[],user:null};
export async function refresh(){[store.batches,store.mappings,store.runs,store.acceptances]=await Promise.all([batches.list(),mappings.list(),runs.list(),acceptances.list()]);}
