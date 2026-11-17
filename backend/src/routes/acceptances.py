from fastapi import APIRouter,Depends
from src.middlewares.auth import current,writer
from src.schemas.business import AcceptanceInput
from src.services import acceptances
from src.services.audit import log
router=APIRouter()
@router.get('/acceptances')
def listing(u=Depends(current)):return acceptances.list_items()
@router.get('/acceptances/{item_id}')
def detail(item_id:int,u=Depends(current)):return acceptances.detail(item_id)
@router.post('/acceptances')
def create(data:AcceptanceInput,u=Depends(writer)):
 row=acceptances.create(data)
 log(u.email,'acceptance.receive',row['acceptance_no']+' seq='+str(row['seq']))
 return row
@router.post('/acceptances/{item_id}/cancel')
def cancel(item_id:int,u=Depends(writer)):
 row=acceptances.cancel(item_id)
 log(u.email,'acceptance.cancel',row['acceptance_no'])
 return row
