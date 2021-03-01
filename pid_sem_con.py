from pymaxdb import conexao
import sys,logging,os,psutil
from imp import reload
diretorio =os.path.dirname(os.path.abspath(sys.argv[0]))

reload(logging)
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',filename=os.path.join(diretorio,'execucao.log'), filemode='w', level=logging.DEBUG)
 # Conexão DBMaker ODBC

par = []
servicos = []



try:
    
    with open(diretorio+'\config.conf','r') as arq:
        for linha in arq:
            conteudo = linha
            if 'DBCONTROL' in linha:
                conteudo = conteudo.replace('\n','') 
                par.append(conteudo)
            else:
                conteudo = conteudo.replace('\n','')
                servicos.append(conteudo)
            
except Exception as e:        
    logging.debug(e)
    sys.exit()

if len(par)+len(servicos) < 2:
    logging.debug('Parâmetros inválidos. Informe conforme o exemplo abaixo: \nDBCONTROL_XXXX_XXX \nServico1\nServico2\nServico3\n...')
    sys.exit()


pid_servicos =[]
processos_vinculados_nssm = []
processos_iscserver = []
processos_java = []
processos_java_pid = []

try:
    for servico in servicos:
    
        wservice = psutil.win_service_get(servico)
        pid = wservice.pid()
        pid_servicos.append(pid)
        processos_vinculados_nssm.append(psutil.Process(pid).children())

    for processo in processos_vinculados_nssm :
        for linha in processo:
            linha =str(linha)
            if 'iscserver.exe' in linha:
                linha = linha.split('=')
                linha = str(linha).split(',')
                linha = str(linha[1]).replace ("'","")
                linha = linha.strip()
                x =(psutil.Process(int(linha)).children())
                processos_iscserver.append(linha)
                processos_java.append(x)
#    print (processos_java)
    
    for processo in processos_java:
        for linha in processo:
            linha=str(linha)
            if 'java.exe' in str(linha):
                linha = linha.split('=')
                linha = str(linha).split(',')
                linha = str(linha[1]).replace ("'","")
                processos_java_pid.append(linha.strip())

            
except Exception as e:
        logging.debug(e)
        sys.exit()


try:
    con = conexao(nome_conexao='dbmakerodbc', db=par[0], usr='XXX', pwd='XXX')
    
    rs = con.consultar("select CONNECTION_ID ID_CONEXAO,\
              CASE WHEN TRIM(cast(SQL_CMD as varchar (400))) LIKE '%(client:%)'\
              THEN  TRIM(substring (SQL_CMD,cast(cast(LOCATE('(client:', cast(SQL_CMD as varchar(400)), 1)\
              as varchar(400)) as integer), 400))  END CLIENTE ,\
              TRIM(login_host)\
              from system.sysuser\
              where TRIM(USER_NAME) NOT LIKE '%SERVER%'  and TRIM(cast(SQL_CMD as varchar (400))) LIKE '%(client: %)'")
except Exception as e:
    logging.debug(e)
    sys.exit()
                   
finally:
    con.fechar()

processos_dbmaker = []

try:
    for item in rs:
    #print (item)
        for i in item:
            if 'client' in str(i):
                a = i.replace('(client: ','').replace(')','')
                processos_dbmaker.append(a)

except Exception as e:
    
    logging.debug(e)
    sys.exit()          
    
    
#print(processos_dbmaker)
#print(processos_java_pid)



processos_finalizar = []

#compara a lista de PID do DBMAKER com os do java.exe e cria uma nova, com os que não constam na lista do dbmaker

processos_finalizar = [x for x in processos_java_pid if x not in processos_dbmaker]
#print(processos_finalizar)

logging.debug('Processos abertos'+ str(processos_finalizar))
print('Processos abertos que não estão conectados no banco'+ str(processos_finalizar))




