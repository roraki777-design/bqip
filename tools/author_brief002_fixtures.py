"""Author normative synthetic vectors independently of BQIP and generated bindings.

Expected semantic values below are explicit contract examples. SHA-256 uses
stdlib hashlib; wire vectors use the Protobuf wire specification directly.
Tests consume committed files, never this authoring program's output at runtime.
"""
import copy
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / 'tests/fixtures/binance-usdm'


def write(name, value):
    (OUT / name).write_text(json.dumps(value, indent=2, ensure_ascii=False) + '\n')


def raw(value):
    return json.dumps(value, separators=(',', ':'), ensure_ascii=False).encode()


def normalized(text):
    whole, dot, fraction = text.partition('.')
    coefficient = (whole + fraction).lstrip('0') or '0'
    scale = len(fraction) if dot else 0
    while scale and coefficient.endswith('0'):
        coefficient = coefficient[:-1]
        scale -= 1
    if not coefficient:
        coefficient, scale = '0', 0
    return {'coefficient': coefficient, 'scale': scale}


TRADE = {'e':'aggTrade','E':2,'s':'BTCUSDT','a':7,'p':'100.00','q':'0.006','nq':'0.004','f':10,'l':12,'T':1,'m':True,'st':1}
DEPTH = {'e':'depthUpdate','E':2,'T':1,'s':'BTCUSDT','U':98,'u':102,'pu':97,'b':[['100','2']],'a':[['101','3']],'ps':'BTCUSDT','st':1}
SNAPSHOT = {'lastUpdateId':100,'E':2,'T':1,'bids':[['100','1'],['99','2']],'asks':[['101','4']]}
SYMBOL = {'symbol':'BTCUSDT','pair':'BTCUSDT','contractType':'PERPETUAL','status':'TRADING','baseAsset':'BTC','quoteAsset':'USDT','marginAsset':'USDT','pricePrecision':7,'quantityPrecision':0,'onboardDate':999,
          'filters':[{'filterType':'PRICE_FILTER','tickSize':'0.10','minPrice':'0.10','maxPrice':'1000000'}, {'filterType':'LOT_SIZE','stepSize':'0.001','minQty':'0.001','maxQty':'100'}, {'filterType':'MARKET_LOT_SIZE','stepSize':'0.002','minQty':'0.002','maxQty':'200'}]}
EXCHANGE = {'symbols':[SYMBOL]}
BASE = {'trade':TRADE,'depth':DEPTH,'snapshot':SNAPSHOT,'exchange':EXCHANGE}
EXPECTED = {
    'trade': {'price':{'coefficient':'100','scale':0},'quantity':{'coefficient':'6','scale':3},'quantity_ex_rpi':{'coefficient':'4','scale':3},'aggregate_id':7,'first_venue_trade_id':10,'last_venue_trade_id':12,'buyer_is_maker':True,'aggressor':'SELL','granularity':'AGGREGATED','liquidation':'NOT_OBSERVED_FROM_THIS_SOURCE','event_timestamp_ns':2000000,'trade_timestamp_ns':1000000},
    'depth': {'first_update_id':98,'final_update_id':102,'previous_final_update_id':97,'bids':[['100:0','2:0']],'asks':[['101:0','3:0']],'event_timestamp_ns':2000000,'transaction_timestamp_ns':1000000},
    'snapshot': {'last_update_id':100,'bids':[['100:0','1:0'],['99:0','2:0']],'asks':[['101:0','4:0']],'event_timestamp_ns':2000000,'transaction_timestamp_ns':1000000},
    'exchange': {'effective_time_basis':'UNKNOWN','effective_from_ns':None,'known_from_ns':5000000,'tick_size':{'coefficient':'1','scale':1},'lot_size':{'coefficient':'1','scale':3},'venue_status':'TRADING','quantity_unit':'BTC','minimum_quantity':{'coefficient':'1','scale':3},'maximum_quantity':{'coefficient':'100','scale':0}},
}
CASES=[]


def case(name, kind, changes=None, error=None, expected=None, drop=(), symbol=False):
    value=copy.deepcopy(BASE[kind])
    target=value['symbols'][0] if symbol else value
    target.update(changes or {})
    for key in drop:
        del target[key]
    want=copy.deepcopy(EXPECTED[kind])
    want.update(expected or {})
    entry={'name':name,'kind':kind,'raw_file':'raw/'+name+'.json','expected':want} if error is None else {'name':name,'kind':kind,'raw_file':'raw/'+name+'.json','error':error}
    payload=raw(value)
    (OUT/entry['raw_file']).write_bytes(payload)
    entry['raw_sha256']=hashlib.sha256(payload).hexdigest()
    CASES.append(entry)


def vi(number):
    output=bytearray()
    while number>127:
        output.append((number&127)|128)
        number >>= 7
    output.append(number)
    return bytes(output)


def field(tag, value):
    if isinstance(value, str):
        value=value.encode()
    if isinstance(value, bytes):
        return vi((tag<<3)|2)+vi(len(value))+value
    return vi(tag<<3)+vi(int(value))


def message(fields):
    return b''.join(field(tag,value) for tag,value in fields)


def dec(text):
    value=normalized(text)
    return message([(1,value['coefficient']),(2,value['scale'])])


def level(p,q):
    return message([(1,dec(p)),(2,dec(q))])


def lineage(kind, profile_hash):
    capture=message([(1,bytes(range(16))),(2,7)])
    return message([(1,'BINANCE-USD_M_FUTURES-BTCUSDT-PERPETUAL-v1'),(2,profile_hash),(3,capture),(4,hashlib.sha256(raw(BASE[kind])).digest()),(5,5000000)])


def context(kind, profile_hash):
    common=lineage(kind,profile_hash)
    if kind in ('trade','depth'):
        stream='btcusdt@aggTrade' if kind=='trade' else 'btcusdt@depth@100ms'
        route='market' if kind=='trade' else 'public'
        return message([(1,common),(2,2),(3,f'wss://fstream.binance.com/{route}/ws/{stream}'),(4,stream),(5,bytes(range(16,32)))])
    endpoint='/fapi/v1/exchangeInfo' if kind=='exchange' else '/fapi/v1/depth'
    fields=[(1,common),(2,1),(3,'GET'),(4,endpoint)]
    if kind=='snapshot':
        fields.extend((5,message([(1,k),(2,v)])) for k,v in [('limit','1000'),('symbol','BTCUSDT')])
    return message([*fields,(6,200)])


def identity(class_name, tail):
    parts=[('venue',b'BINANCE'),('product_family',b'USD_M_FUTURES'),('symbol',b'BTCUSDT'),('event_class',class_name.encode()),*tail]
    pre=b'BQIP-ID-V1'+b''.join(len(k.encode()).to_bytes(4,'big')+k.encode()+len(v).to_bytes(4,'big')+v for k,v in parts)
    return {'components':[[k,v.hex()] for k,v in parts],'preimage_hex':pre.hex(),'sha256':hashlib.sha256(pre).hexdigest()}


def main():
    (OUT/'raw').mkdir(parents=True,exist_ok=True)
    case('exchange-valid','exchange')
    for key,value in [('contractType','CURRENT_QUARTER'),('marginAsset','BTC'),('pair','ETHUSDT'),('baseAsset','ETH'),('quoteAsset','USDC')]:
        case('exchange-wrong-'+key,'exchange',{key:value},'PROFILE_MISMATCH',symbol=True)
    case('exchange-halt','exchange',{'status':'HALT'},expected={'venue_status':'HALT'},symbol=True)
    case('exchange-price-precision','exchange',{'pricePrecision':0},symbol=True)
    case('exchange-quantity-precision','exchange',{'quantityPrecision':9},symbol=True)
    case('exchange-missing-price','exchange',{'filters':SYMBOL['filters'][1:]},'METADATA_INCOMPLETE',symbol=True)
    case('exchange-missing-lot','exchange',{'filters':[SYMBOL['filters'][0],SYMBOL['filters'][2]]},'METADATA_INCOMPLETE',symbol=True)
    case('exchange-onboard-not-effective','exchange',{'onboardDate':1},symbol=True)
    case('exchange-duplicate-filter','exchange',{'filters':[*SYMBOL['filters'],SYMBOL['filters'][0]]},'METADATA_INCOMPLETE',symbol=True)
    case('exchange-duplicate-symbol','exchange',{'symbols':[SYMBOL,SYMBOL]},'METADATA_INCOMPLETE')
    case('exchange-missing-status','exchange',drop=('status',),error='PARSE_FAILURE',symbol=True)
    case('trade-sell','trade')
    case('trade-buy','trade',{'m':False},expected={'buyer_is_maker':False,'aggressor':'BUY'})
    case('trade-single-member','trade',{'l':10},expected={'last_venue_trade_id':10})
    case('trade-no-rpi','trade',{'nq':'0.006'},expected={'quantity_ex_rpi':{'coefficient':'6','scale':3}})
    case('trade-st-one','trade',{'st':1})
    case('trade-st-two','trade',{'st':2},'PROFILE_MISMATCH')
    case('trade-no-st','trade',drop=('st',))
    case('trade-malformed-decimal','trade',{'q':'1e-3'},'INVALID_VENUE_DECIMAL')
    case('trade-reversed-range','trade',{'f':13},'INVALID_TRADE_RANGE')
    case('trade-negative-quantity','trade',{'q':'-1'},'INVALID_VENUE_DECIMAL')
    case('trade-nq-too-large','trade',{'nq':'1'},'INVALID_RPI_QUANTITY')
    case('trade-nq-zero','trade',{'nq':'0'},expected={'quantity_ex_rpi':{'coefficient':'0','scale':0}})
    case('trade-missing-nq','trade',drop=('nq',),error='INVALID_VENUE_DECIMAL')
    case('trade-time-overflow','trade',{'T':9223372036855},'TIMESTAMP_OVERFLOW')
    case('trade-bool-id','trade',{'a':True},'PARSE_FAILURE')
    case('trade-float-id','trade',{'a':7.0},'PARSE_FAILURE')
    case('trade-float-quantity','trade',{'q':0.006},'INVALID_VENUE_DECIMAL')
    case('trade-unknown-additive','trade',{'newMetric':1.5})
    case('trade-multiple-members','trade',{'l':100},expected={'last_venue_trade_id':100})
    case('depth-ordinary','depth')
    case('depth-several-levels','depth',{'b':[['100','2'],['99','3']]},expected={'bids':[['100:0','2:0'],['99:0','3:0']]})
    case('depth-zero-delete','depth',{'b':[['100','0']]},expected={'bids':[['100:0','0:0']]})
    case('depth-absent-delete','depth',{'b':[['50','0']]},expected={'bids':[['50:0','0:0']]})
    case('depth-absolute','depth',{'b':[['100','7']]},expected={'bids':[['100:0','7:0']]})
    case('depth-st-one','depth',{'st':1})
    case('depth-st-two','depth',{'st':2},'PROFILE_MISMATCH')
    case('depth-wrong-pair','depth',{'ps':'ETHUSDT'},'PROFILE_MISMATCH')
    case('depth-malformed-level','depth',{'b':[['100']]},'INVALID_LEVEL')
    case('depth-reversed-range','depth',{'U':103},'INVALID_DEPTH_RANGE')
    case('depth-no-migration-fields','depth',drop=('st','ps'))
    case('depth-time-overflow','depth',{'E':9223372036855},'TIMESTAMP_OVERFLOW')
    case('depth-negative-price','depth',{'b':[['-100','2']]},'INVALID_VENUE_DECIMAL')
    case('depth-missing-pu','depth',drop=('pu',),error='PARSE_FAILURE')
    case('depth-unknown-additive','depth',{'future':{'x':1.5}})
    case('snapshot-valid','snapshot')
    case('snapshot-empty','snapshot',{'bids':[],'asks':[]},expected={'bids':[],'asks':[]})
    case('snapshot-missing-sequence','snapshot',drop=('lastUpdateId',),error='PARSE_FAILURE')
    case('snapshot-time-overflow','snapshot',{'E':9223372036855},'TIMESTAMP_OVERFLOW')
    write('parsing.json',CASES)
    write('boundaries.json',[
        {'name':'u-S-minus-one','U':98,'u':99,'S':100,'state':'BRIDGING'},
        {'name':'u-equals-S','U':98,'u':100,'S':100,'state':'HEALTHY'},
        {'name':'U-and-u-equal-S','U':100,'u':100,'S':100,'state':'HEALTHY'},
        {'name':'U-less-u-greater','U':98,'u':102,'S':100,'state':'HEALTHY'},
        {'name':'U-equals-u-greater','U':100,'u':102,'S':100,'state':'HEALTHY'},
        {'name':'U-after-S','U':101,'u':102,'S':100,'state':'BRIDGING'}])
    write('sync.json',{'snapshot':SNAPSHOT,'events':[
        {**DEPTH,'U':95,'u':99,'pu':94,'b':[['50','999']]},
        {**DEPTH,'b':[['100','2'],['50','0']],'a':[['101','0'],['102','5']]},
        {**DEPTH,'U':103,'u':104,'pu':102,'b':[['100','7']],'a':[]},
        {**DEPTH,'U':105,'u':106,'pu':999,'b':[['100','999']],'a':[]}],
        'after_bridge':{'bids':[['100:0','2:0'],['99:0','2:0']],'asks':[['102:0','5:0']]},
        'after_next':{'bids':[['100:0','7:0'],['99:0','2:0']],'asks':[['102:0','5:0']]},
        'transitions':['EMPTY','BUFFERING','SNAPSHOT_REQUIRED','BRIDGING','HEALTHY','DEGRADED','RESYNC_REQUIRED'],'error':'VENUE_GAP'})
    unknown={'instrument_id':'TEST','metadata_version':'v1','known_from_ns':10,'product_type':'LINEAR','margin_asset':'USDT','settlement_asset':'USDT','effective_time_basis':3}
    confirmed={**unknown,'metadata_version':'v2','known_from_ns':30,'effective_from_ns':5,'effective_time_basis':2,'supersedes_version':'v1'}
    tests=[]
    def temporal(name, versions, mode='AS_LIVED', event=6, receive=10, expected='v1', basis=2, error=None):
        tests.append({'name':name,'versions':versions,'mode':mode,'event_time_ns':event,'as_of_ns':receive,
                      **({'error':error} if error else {'expected':expected,'basis':basis,'quality':'METADATA_EFFECTIVE_TIME_UNKNOWN' if basis==2 else None})})
    temporal('unknown-accepted',[unknown])
    temporal('unknown-with-start',[{**unknown,'effective_from_ns':0}],error='UNKNOWN_EFFECTIVE_START_MUST_BE_ABSENT')
    temporal('known-without-start',[{**unknown,'effective_time_basis':1}],error='MISSING_METADATA_TIME')
    temporal('before-known',[unknown],receive=9,error='METADATA_MISSING')
    temporal('known-boundary',[unknown],receive=10)
    temporal('knowledge-only',[unknown],event=1,receive=11)
    temporal('corrected-unknown',[unknown],mode='CORRECTED_RESEARCH',error='METADATA_EFFECTIVE_TIME_UNKNOWN')
    temporal('corrected-confirmed',[unknown,confirmed],mode='CORRECTED_RESEARCH',expected='v2',basis=1)
    temporal('forensic-prior',[unknown,confirmed],receive=20)
    temporal('forensic-later',[unknown,confirmed],receive=30,expected='v2',basis=1)
    temporal('unrelated-overlap',[unknown,{**unknown,'metadata_version':'other'}],error='METADATA_AMBIGUOUS')
    temporal('exclusive-known-end',[{**unknown,'known_to_ns':20}],receive=20,error='METADATA_MISSING')
    temporal('unspecified-basis',[{**unknown,'effective_time_basis':0}],error='INVALID_EFFECTIVE_TIME_BASIS')
    temporal('corrected-no-interval',[unknown,confirmed],mode='CORRECTED_RESEARCH',event=4,error='METADATA_EFFECTIVE_TIME_UNKNOWN')
    temporal('as-of-required',[unknown],receive=None,error='AS_OF_REQUIRED')
    temporal('missing-known',[{k:v for k,v in unknown.items() if k!='known_from_ns'}],error='MISSING_METADATA_TIME')
    write('metadata.json',tests)
    session=bytes(range(16,32))
    ids=[identity('AGG_TRADE',[('a',b'7')]),identity('AGG_TRADE',[('a',b'0')]),
         identity('DEPTH_UPDATE',[('session_id',session),('U',b'98'),('u',b'102')]),
         identity('DEPTH_UPDATE',[('session_id',bytes(range(32,48))),('U',b'98'),('u',b'102')])]
    write('identity.json',ids)
    profile=json.loads((ROOT/'config/venues/binance/usdm/btcusdt-perpetual-v1.json').read_text())
    jcs=json.dumps(profile,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
    profile_hash=hashlib.sha256(jcs).digest()
    write('profile.json',{'input':profile,'canonical_hex':jcs.hex(),'sha256':profile_hash.hex()})
    trade_fields=[(1,'BINANCE:USD_M_FUTURES:BTCUSDT'),(2,'7'),(3,bytes.fromhex(ids[0]['sha256'])),(4,dec('100')),(5,dec('0.006')),(6,2000000),(7,1000000),(8,2),(9,1),(10,2),(11,2),(12,7),(13,10),(14,12),(15,dec('0.004')),(16,True),(17,1),(18,context('trade',profile_hash))]
    depth_fields=[(1,98),(2,102),(3,97),(4,2),(5,1),(6,2000000),(7,1000000),(8,level('100','2')),(9,level('101','3')),(10,bytes.fromhex(ids[2]['sha256'])),(11,context('depth',profile_hash))]
    snapshot_fields=[(1,100),(2,2),(3,1),(4,2000000),(5,1000000),(6,level('100','1')),(6,level('99','2')),(7,level('101','4')),(8,context('snapshot',profile_hash))]
    metadata_fields=[(1,'BINANCE:USD_M_FUTURES:BTCUSDT'),(2,'observation-1'),(5,5000000),(9,'BTC'),(10,dec('0.1')),(11,dec('0.001')),(12,'LINEAR'),(13,'USDT'),(14,'USDT'),(15,3),(16,'BINANCE'),(17,'USD_M_FUTURES'),(18,'BTCUSDT'),(19,'BTCUSDT'),(20,'PERPETUAL'),(21,'BTC'),(22,'USDT'),(23,'TRADING'),(24,context('exchange',profile_hash)),(25,dec('0.001')),(26,dec('100'))]
    vectors=[]
    for kind,fields in [('trade',trade_fields),('depth',depth_fields),('snapshot',snapshot_fields),('exchange',metadata_fields)]:
        encoded=message(fields)
        name={'trade':'trade-sell','depth':'depth-ordinary','snapshot':'snapshot-valid','exchange':'exchange-valid'}[kind]
        vectors.append({'kind':kind,'raw_file':'raw/'+name+'.json','wire_hex':encoded.hex(),'sha256':hashlib.sha256(encoded).hexdigest()})
    write('wire.json',vectors)
    write('enums.json',{
        'EffectiveTimeBasis':{'UNSPECIFIED':0,'VENUE_DECLARED':1,'BQIP_CONFIRMED':2,'UNKNOWN':3},
        'MetadataResolutionBasis':{'UNSPECIFIED':0,'EFFECTIVE_INTERVAL':1,'KNOWLEDGE_ONLY':2},
        'TradeGranularity':{'UNKNOWN':0,'INDIVIDUAL':1,'AGGREGATED':2},
        'AggressorSide':{'UNKNOWN':0,'BUY':1,'SELL':2},
        'LiquidationObservation':{'UNKNOWN':0,'NOT_OBSERVED_FROM_THIS_SOURCE':1},
        'SourceTransport':{'UNSPECIFIED':0,'HTTP_REST':1,'WEBSOCKET':2},
        'CauseCode_additions':{'VENUE_IDENTITY_CONFLICT':11,'METADATA_EFFECTIVE_TIME_UNKNOWN':12,'METADATA_INCOMPLETE':13,'PROFILE_MISMATCH':14}})
    print(f'Authored {len(CASES)} parsing, {len(tests)} temporal, 6 boundary, 4 identity, 4 wire vectors; synthetic only.')


if __name__=='__main__':
    main()
