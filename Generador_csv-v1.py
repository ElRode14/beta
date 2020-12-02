import time
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler
import cx_Oracle
import getpass
import shutil

log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
logFile = 'generador.log'
my_handler = RotatingFileHandler(logFile, mode='w', maxBytes=2*1024*1024, backupCount=2, encoding=None, delay=0)
my_handler.setFormatter(log_formatter)
my_handler.setLevel(logging.INFO)
app_log = logging.getLogger()
app_log.setLevel(logging.INFO)
app_log.addHandler(my_handler)

QUERY_TLC="""SELECT spi.TIMESTAMP AS PI_TIMESTAMP,Concat(spi.external_identity,'.SV') AS "PI_TAGNAME1",spi.Value AS "PI_VALUE1",'0' AS "PI_STATUS1",Concat(spi.external_identity,'.SQ') AS "PI_TAGNAME2",spi.Quality AS "PI_VALUE2",'0' AS "PI_STATUS2" FROM tlc.scada_pi spi """

def Q_TLC_PI(fechafin,usernameTLC,passwordTLC,SIDTLC='TLCPR01',cantidaddias=1,QUERY_TLC=QUERY_TLC):
	pathf='Repositorio/'
	finalizado='Repositorio/Finalizado/'
	connection = None
	cursor = None
	datosclient=None
	tiempoinicio0=datetime.now()
	for di in range(cantidaddias):
		tiempoinicio=datetime.now()
		parseo=0
		finicio=datetime.strftime(datetime.strptime(str(fechafin),"%Y%m%d")-timedelta(days=di+1),"%Y%m%d")
		fechafine=datetime.strftime(datetime.strptime(str(fechafin),"%Y%m%d")-timedelta(days=di),"%Y%m%d")
		archivo=str(finicio)+'.csv'
		logging.info(f'inicio de la carga de {archivo}')
		try:
			dsn_tns = cx_Oracle.makedsn('webdbpr01',1525,service_name=SIDTLC)
			connection = cx_Oracle.connect(usernameTLC,passwordTLC,dsn_tns,encoding='UTF-8')
			consultaquery=(QUERY_TLC+" where SRVTIME between tlc.scada.date_to_milli(to_date('{}','yyyymmdd')) and tlc.scada.date_to_milli(to_date('{}','yyyymmdd'))").format(finicio,fechafine)
			#consultaquery="""SELECT spi.TIMESTAMP AS PI_TIMESTAMP,Concat(spi.external_identity,'.SV') AS "PI_TAGNAME1",spi.Value AS "PI_VALUE1",'0' AS "PI_STATUS1",Concat(spi.external_identity,'.SQ') AS "PI_TAGNAME2",spi.Quality AS "PI_VALUE2",'0' AS "PI_STATUS2" FROM tlc.scada_pi spi where SRVTIME between tlc.scada.date_to_milli(to_date('20201001','yyyymmdd')) and tlc.scada.date_to_milli(to_date('20201002','yyyymmdd')) - 1"""
			#logging.info(f'Query = {consultaquery}')
			#Abro cursor
			cursor = connection.cursor()
			cursor.prefetchrows = 2100
			cursor.arraysize = 2000
			#Ejecuto cursor
			cursor.execute(consultaquery)
			while True:
				if parseo==0:
					filelectura=pathf+archivo
					f = open(filelectura,"w")
					f.write('PI_TIMESTAMP;PI_TAGNAME1;PI_VALUE1;PI_STATUS1;PI_TAGNAME2;PI_VALUE2;PI_STATUS2\n')
					parseo=1
				rows = cursor.fetchmany()
				if rows:
					for i in range(0,len(rows)):
						#print(rows[i][0])
						f.write(rows[i][0].strftime('%d-%m-%Y %H:%M:%S.%f')+';'+str(rows[i][1])+';'+str(rows[i][2])+';'+str(rows[i][3])+';'+str(rows[i][4])+';'+str(rows[i][5])+';'+str(rows[i][6])+'\n')
				else:
					f.close()
					shutil.move(filelectura, finalizado+archivo)
					break
				#targetCursor.executemany("insert into MyTable values (:1, :2)", rows)
				#targetConnection.commit()
		#Traigo todos los registros
		#datosclient=cursor.fetchall() #Tupla con todos los diccionarios, cada uno es un registro de clientes
		except cx_Oracle.Error as error:
			logging.error(f'Query = {consultaquery}')
			logging.error(f'Error conexión con base Oracle a las {fechainicio}')
			return tiempoinicio
		finally:
	#		# release the connection
			if cursor:
				cursor.close()
			if connection:
				connection.close()
		tiempoproceso=((datetime.now()-tiempoinicio)/timedelta(minutes=1))
		logging.info(f'Terminada la carga de {archivo} en {tiempoproceso} minutos')
	tiempoproceso=((datetime.now()-tiempoinicio0)/timedelta(minutes=1))
	return tiempoproceso

if __name__ == '__main__':
	usernameTLC=input("Ingrese usuario conexión a base Scada: ")
	passwordTLC=getpass.getpass("Ingrese password conexión a base Scada:")
	SIDTLC=input("Ingrese SID de conexión a base Scada: ")
	if SIDTLC=="":
		SIDTLC='TLCPR01'
	fechafin=int(input("Ingrese fecha fin consulta (formato yyyymmdd): "))
	cantidaddias=int(input("Ingrese cantidad de dias a consultar: "))
	fechainicio=datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
	logging.info(f'Inicio proceso {fechainicio}')
	Respuesta=Q_TLC_PI(fechafin=fechafin,usernameTLC=usernameTLC,passwordTLC=passwordTLC,cantidaddias=cantidaddias,QUERY_TLC=QUERY_TLC)
	logging.info(f'Terminada la carga de '+str(cantidaddias)+' dias. Proceso iniciando el '+fechainicio+' con duración total de '+str(Respuesta)+' minutos')
