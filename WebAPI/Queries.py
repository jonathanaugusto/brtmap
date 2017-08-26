from dbconnect import connection

transoeste = ["10","11","12","13","14","15","17","18","19","20","20A","21","22","23","24"]
transcarioca = ["30","31","33","35","36","38","40","41","42"]
transolimpica = ["41","50","51","53"]
lote_zero = ["18","21","22","23","24","40","50","53"]
linhas = '("10","11","12","13","14","15","17","18","19","20","20A","21","22","23","24","30","31","33","35","36","38","40","41","42","41","50","51","53","18","21","22","23","24","40","50","53")'

def brtPosQuery(linha = None):
    c, conn = connection()
    query = ""
    if linha == "Linha":
        query = "SELECT latitude, longitude, trajeto, velocidade, corredor FROM gpsdata WHERE datahora > timestamp(DATE_SUB(NOW(), INTERVAL 3 MINUTE))"
    else:
        query = "SELECT latitude, longitude, trajeto, velocidade, corredor FROM gpsdata WHERE datahora > timestamp(DATE_SUB(NOW(), INTERVAL 3 MINUTE)) AND corredor = '{0}'".format(linha)
        
    result_finalize = []
    result_set_country = c.execute(query.format(linhas))
    
    for r in c:
        lat, long, title, velocidade, corredor = r
        result_finalize.append({"lat":lat, "lng":long, "title":title, "count" : velocidade, "icon":get_icon(corredor)})
    return result_finalize
    
def get_icon(linha):
    if linha == "TransCarioca":
        return "../static/transcarioca.png"
    if linha == "TransOlímpica":
        return "../static/transolimpica.png"
    if linha == "TransOeste":
        return "../static/transoeste.png"

def heatMapQuery(linha=None):
    c, conn = connection()
    
    c, conn = connection()
    query = ""
    if linha == "Linha":
        query = "SELECT latitude, longitude, trajeto, velocidade, corredor FROM gpsdata WHERE datahora > timestamp(DATE_SUB(NOW(), INTERVAL 7 HOUR))"
    else:
        query = "SELECT latitude, longitude, trajeto, velocidade, corredor FROM gpsdata WHERE datahora > timestamp(DATE_SUB(NOW(), INTERVAL 7 HOUR)) AND corredor = '{0}'".format(linha)
        
    result_finalize = []
    result_set_country = c.execute(query.format(linhas))
    
    for r in c:
        lat, long, title, velocidade, corredor = r
        result_finalize.append({"lat":lat, "lng":long, "title":title, "count" : velocidade, "icon":get_icon(corredor)})
    return result_finalize