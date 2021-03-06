from http.server import * 
from io import *
import pymysql
import sys
import ast
import base64
import ssl
class RestifHandler(BaseHTTPRequestHandler):
    def Send(self,status,mess):
        self.send_response(status)
        if self.origin is not None:
            self.send_header("Access-Control-Allow-Origin",self.origin)
        self.send_header("Cache-control", "no-store");
        self.send_header("Expires", "-1");
        self.send_header("Pragma", "no-cache");
        if mess!=None:
            bs = bytes(mess,'utf-8')
            self.send_header('Content-Length',len(bs))
            self.end_headers()
            self.wfile.write(bs) 
        return
    ## WWW-authentication override
    def handle_one_request(self):
        try:
            self.raw_requestline = self.rfile.readline(65537)
            if len(self.raw_requestline) > 65536:
                self.requestline = ''
                self.request_version = ''
                self.command = ''
                self.send_error(414)
                return
            if not self.raw_requestline:
                self.close_connection = True
                return
            if not self.parse_request():
                # An error code has been sent, just exit
                return
            # code for our override
            self.origin = self.headers["Origin"]
            h = self.headers["Authorization"]
            if h is None:
              self.send_error401()
              return
            d = str(base64.b64decode(h[6:len(h)]),'utf-8') # 6 is len('Basic ')
            s = d.split(':')
            self.usr = s[0]
            self.pwd = s[1]
            # end of our special code
            mname = 'do_' + self.command
            if not hasattr(self, mname):
                self.send_error(
                    501,
                    "Unsupported method (%r)" % self.command)
                return
            method = getattr(self, mname)
            method()
            self.wfile.flush() #actually send the response if not already done.
        except socket.timeout as e:
            #a read or a write timed out.  Discard this connection
            self.log_error("Request timed out: %r", e)
            self.close_connection = True
            return
    def send_error401(self):
        self.send_response(401, None)
        self.send_header('Connection', 'close')
        self.send_header("WWW-Authenticate",'Basic realm="MySQL login"')
        body = None
        self.end_headers()
        return
    def do_OPTIONS(self):
        h = self.headers["Access-Control-Request-Headers"]
        self.send_response(200, None)
        self.send_header('Connection', 'close')
        self.send_header("WWW-Authenticate",'Basic realm="MySQL login"')
        self.send_header("Access-Control-Allow-Headers",h)
        self.send_header("Access-Control-Allow-Origin",self.origin)
        self.send_header("Access-Control-Allow-Methods","GET, POST, PUT, DELETE, OPTIONS")
        self.send_header("Access-Control-Max-Age","86400")
        body = None
        self.end_headers()
        return
    def do_GET(self):
        try:
            segments = self.path.split('/') 
            db = None
            tb = None
            wh = None
            if segments[1]!='':
                db = segments[1]
            if len(segments)>2:
                tb = segments[2]
            if len(segments)>3:
                wh = segments[3]
            conn = None
            page = db is not None and (db.endswith('.htm') or db.endswith('.js'))
            if page:
                mess = open(db,'rb',0).readall().decode('utf-8')
            else:
                conn = pymysql.connect(user=self.usr,password=self.pwd,database=db)
                if conn is None:
                    self.Send(400,'Cannot connect')
                    return
                if tb==None:
                    mess = self.Get1(conn,db)
                else:
                    mess = self.Get2(conn,tb,wh)
            self.Send(200,mess)
        except Exception as e:
            self.Send(403,e.msg)
        if conn!=None:
            conn.close()
        return
    def Get1(self,conn,db):
        cursor = conn.cursor(raw=True)
        if db is None:
           cursor.execute('show databases')
        else:
            cursor.execute('show tables')
        mess = str()
        for rw in cursor.fetchall():
            mess += rw[0].decode('utf-8') + '\n'
        cursor.close()
        return mess
    def Get2(self,conn,tb,wh):
        cursor = conn.cursor()
        if wh is None:
            cursor.execute('Select * from '+tb)
        else:
            cursor.execute('Select * from '+tb+' where '+wh)
        rows = cursor.fetchall()
        desc = cursor._description
        acc = self.headers['Accept']
        fmts = { 'text/plain':['','\n','','',',',''], # array start mid end; obj start mid end
                'application/json':['[',',\n',']','{',', ','}'],
                'application/xml':['<root>','','</root>','<'+tb+'>','','</'+tb+'>'],
                'text/html':['','','</table>','<tr>','','</tr>']
                }
        fmt = fmts[acc]
        mess = []
        if acc=='text/html':
            mess.append('<table border><tr>')
            for hd in desc:
                mess.append('<th>'+hd[0]+'</th>')
            mess.append('</tr>') 
        mess.append(fmt[0]) # array start
        rsep = ''     # row separator initially empty
        for rw in rows:
            mess.append(rsep) 
            rsep = fmt[1] # row sep is array mid
            mess.append(fmt[3]) # obj start
            fsep = ''
            for i in range(len(desc)):
                mess.append(fsep)
                fsep = fmt[4] # obj mid
                mess.append(self.Field(acc,desc,rw,i)) 
            mess.append(fmt[5]) # obj end
        cursor.close()
        mess.append(fmt[2]) # array end
        return ''.join(mess)
    def Field(self,acc,desc,data,i):
        if acc=='text/html':
            return '<td>'+str(data[i])+'</td>'
        v = self.Value(desc[i][1],str(data[i]))
        if acc=='text/plain':
            return v
        if acc=='application/json':
            return '"' + desc[i][0] +'": ' + v

        return '<'+desc[i][0]+'>'+v+'</'+desc[i][0]+'>' #xml
    def Value(self,typ,val):
        if typ in {253,254,252,10,7,11}: # char, varchar, text. date, timestamp, time
            return "'" + val.replace("'","''") +"'" # single quote because Sql option uses text/plain too
        return val
    def log_request(code,size):
        return
    def GetData(self):
        if not self.headers.__contains__('Content-Length'):
            return None
        n = int(self.headers['Content-Length'])
        return str(self.rfile.read(n),'utf-8')
    def do_POST(self):
        segments = self.path.split('/') 
        db = None
        tb = None
        if segments[1]!='':
            db = segments[1]
        if len(segments)>2:
            tb = segments[2]
        data = self.GetData()
        if data is None:
            self.Send(400,'Nothing to do')
            return
        conn = None
        conn = pymysql.connect(user=self.usr,password=self.pwd,database=db)
        if conn is None:
            self.Send(400,'Cannot connect')
            return
        if tb is None:
            self.Post1(conn,data)
        else:
            self.Post2(conn,tb,data)
        conn.close()
        return
    def Post1(self,conn,data):
        try:    
            conn.start_transaction(consistent_snapshot=True,isolation_level='SERIALIZABLE')
            k = 1
            for result in conn.cmd_query_iter(data):
                k = k+1
            conn.commit()
            self.Send(200,'OK')
        except Exception as e:
            self.Send(403,'line '+str(k)+': '+e.msg)
        return
    def Post2(self,conn,tb,data):
        try:
            data = ast.literal_eval(data)
            cmd = ['insert into '+tb+' (']
            vals = [') values (']
            cm = '' 
            for k in data.keys():
                cmd.append(cm+k)
                v = data[k]
                q = ''
                if type(v) is str:
                    q = "'"
                vals.append(cm+q+str(v)+q)
                cm = ','
            conn.cmd_query(''.join(cmd)+''.join(vals)+')')
            conn.commit()
            self.Send(200,'OK')
        except Exception as e:
            self.Send(403,e.msg)
        return
    def do_PUT(self):
        segments = self.path.split('/') 
        db = None
        tb = None
        wh = None
        if segments[1]!='':
            db = segments[1]
        if len(segments)>2:
            tb = segments[2]
        if len(segments)>>3:
            wh = segments[3]
        data = self.GetData()
        if data is None:
            self.Send(400,'Nothing to do')
            return
        conn = None
        conn = pymysql.connect(user=self.usr,password=self.pwd,database=db)
        if conn is None:
            self.Send(400,'Cannot connect')
            return
        try:
            data = ast.literal_eval(data)
            cmd = ['update '+tb+' set ']
            cm = '' 
            for k in data.keys():
                v = data[k]
                q = ''
                if type(v) is str:
                    q = "'"
                cmd.append(cm+k+'='+q+str(v)+q)
                cm = ','
            conn.cmd_query(''.join(cmd)+' where '+wh)
            conn.commit()
            self.Send(200,'OK')
        except Exception as e:
            self.Send(403,e.msg)
        conn.close()
        return
    def do_DELETE(self):
        segments = self.path.split('/') 
        db = None
        tb = None
        wh = None
        if segments[1]!='':
            db = segments[1]
        if len(segments)>2:
            tb = segments[2]
        if len(segments)>>3:
            wh = segments[3]
        conn = None
        conn = pymysql.connect(user=self.usr,password=self.pwd,database=db)
        if conn is None:
            self.Send(400,'Cannot connect')
            return
        try:
            data = ast.literal_eval(data)
            cmd = 'delete from '+tb
            if wh is not None:
                cmd += ' where '+wh
            conn.cmd_query(cmd)
            conn.commit()
            self.Send(200,'OK')
        except Exception as e:
            self.Send(403,e.msg)
        conn.close()
        return
try:
    server = HTTPServer(('',4438),RestifHandler)
    server.socket = ssl.wrap_socket(server.socket,certfile="restif.crt",
                                    keyfile="restif.key",server_side=True)
    server.serve_forever()
except KeyboardInterrupt:
    print ('Exiting')
    server.socket.close()

