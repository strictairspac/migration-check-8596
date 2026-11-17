export const ROLES={admin:'管理员',operator:'操作员',viewer:'观察员'};
export const STATUSES={queued:'等待处理',running:'迁移中',completed:'已完成',failed:'失败',cancelled:'已取消'};
export const ACCEPTANCE_STATUS={pending:'已受理待派发',running:'迁移中',completed:'已完成',failed:'失败',cancelled:'已取消'};
export const WAIT_REASONS={newly_accepted:'刚受理，等待派发',waiting_dispatch:'等待执行者派发',waiting_predecessor:'同批次前项未终结，按受理顺序排队',running:null};
export const TYPES={sample_code:'text',test_name:'text',result_value:'decimal',unit:'text',measured_at:'datetime'};
