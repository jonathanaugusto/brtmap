from dbconnect import connection

transoeste = ["10","11","12","13","14","15","17","18","19","20","20A","21","22","23","24"]
transcarioca = ["30","31","33","35","36","38","40","41","42"]
transolimpica = ["41","50","51","53"]
lote_zero = ["18","21","22","23","24","40","50","53"]
linhas = '("10","11","12","13","14","15","17","18","19","20","20A","21","22","23","24","30","31","33","35","36","38","40","41","42","41","50","51","53","18","21","22","23","24","40","50","53")'

def brtPosQuery(linha):
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
    
    query = ""
    if linha == "Linha":
        query = "SELECT latitude, longitude, trajeto, velocidade, corredor FROM gpsdata WHERE datahora > timestamp(DATE_SUB(NOW(), INTERVAL 10 MINUTE))"
    else:
        query = "SELECT latitude, longitude, trajeto, velocidade, corredor FROM gpsdata WHERE datahora > timestamp(DATE_SUB(NOW() from gpsdata), INTERVAL 10 MINUTE)) AND corredor = '{0}'".format(linha)
        
    result_finalize = []
    result_set_country = c.execute(query.format(linhas))
    
    for r in c:
        lat, long, title, velocidade, corredor = r
        result_finalize.append({"lat":lat, "lng":long, "title":title, "count" : velocidade, "icon":get_icon(corredor)})
    return result_finalize

def averageSpeedQuery(per_day = True):
    c, conn = connection()
    query = ""
    if per_day:
        query = "select hora, corredor, max(vel_media) from stats_vel where data > DATE_SUB(NOW(), INTERVAL 24 HOUR) group by hora, corredor order by hora"
    else:
        query = "select data, corredor, max(vel_media) from stats_vel where data > DATE_SUB(NOW(), INTERVAL 6 DAY) group by data, corredor order by data"
    
    result_finalize = []    
    result_set = c.execute(query)
    
    transOlimpica = {"name":"TransOlímpica", 
                        "data":[]
                    }
    transCarioca = {"name":"TransCarioca", 
                        "data":[]
                    }
    transOeste = {"name":"TransOeste", 
                        "data":[]
                    }
    categoria = []
    for r in c:
        data, corredor, vel = r
        if corredor == 'TransOeste':
            transOeste.get("data").append(str(vel))
        if corredor == 'TransCarioca':
            transCarioca.get("data").append(str(vel))
        if corredor == 'TransOlímpica':
            transOlimpica.get("data").append(str(vel))
        if str(data) not in categoria:
            categoria.append(str(data))
            
    query_result = {"content":[transCarioca, transOeste, transOlimpica], 
                    "categories":categoria}
    return query_result

def availableBusQuery(per_day = True):
    c, conn = connection()
    query = ""
    if per_day:
        query = "select hora, corredor, max(qtd_carros) from stats_qtd where data > DATE_SUB(NOW(), INTERVAL 24 HOUR) group by hora, corredor order by hora"
    else:
        query = "select data, corredor, max(qtd_carros) from stats_qtd where data > DATE_SUB(NOW(), INTERVAL 6 DAY) group by data, corredor order by data"
    
    result_finalize = []    
    result_set = c.execute(query)
    
    transOlimpica = {"name":"TransOlímpica",  
                        "data":[]
                    }
    transCarioca = {"name":"TransCarioca", 
                        "data":[]
                    }
    transOeste = {"name":"TransOeste", 
                  "data":[]
                    }
    categoria = []
    for r in c:
        data, corredor, qtd = r
        if corredor == 'TransOeste':
            transOeste.get("data").append(str(qtd))
        if corredor == 'TransCarioca':
            transCarioca.get("data").append(str(qtd))
        if corredor == 'TransOlímpica':
            transOlimpica.get("data").append(str(qtd))
        if str(data) not in categoria:
            categoria.append(str(data))
            
    query_result = {"content":[transCarioca, transOeste, transOlimpica], 
                    "categories":categoria}
    return query_result
