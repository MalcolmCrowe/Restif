﻿using System;
using System.Text;
using System.Net;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using System.IO;
namespace RestifD
{
    class Restif
    {
        HttpListenerContext context;
        static string host = "localhost";
        MySqlConnection conn = null;
        static void Main(string[] args)
        {
            if (args.Length > 0)
                host = args[0];
            var listener = new HttpListener();
            listener.Prefixes.Add("http://" + host + ":8078/");
            listener.AuthenticationSchemes = AuthenticationSchemes.Basic;
            listener.Start();
            for (;;)
                try
                {
                    new Restif { context = listener.GetContext() }.Run();
                }
                catch(Exception e)
                {
                    Console.WriteLine(e.Message);
                    break;
                }
        }
        string get(string[] p, int i)
        {
            return (i < p.Length) ? (p[i] != "") ? p[i].Trim('/') : null : null;
        }
        void Run()
        {
            Task.Run(() =>
          {
                var meth = context.Request.HttpMethod;
                var path = context.Request.Url.Segments;
                var h = context.Request.Headers["Authorization"];
                var s = Encoding.UTF8.GetString(Convert.FromBase64String(h.Substring(6))).Split(':');
                string db = get(path, 1);
                string tb = get(path, 2);
                string wh = get(path, 3);
                var connstring = "server=" + host + ";uid="+s[0]+";password="+s[1];
                if (db != null)
                    connstring += ";database=" + db;
                string mess = null;
                try
                {
                    conn = new MySqlConnection(connstring);
                    conn.Open();
                    switch (meth)
                    {
                        case "GET":
                            mess = (tb==null)?Get1(db):Get2(tb,wh);
                            break;
                        case "POST":
                            var pd = Receive();
                            if (tb != null)
                                mess = Post2(tb, new Document(pd));
                            else if (db != null)
                                mess = Post1(pd);
                            else
                                throw new Exception("not supported");
                            break;
                        case "PUT":
                            if (db == null || tb == null || wh == null)
                                throw new Exception("not supported");
                            mess = Put(tb,wh,new Document(Receive()));
                            break;
                        case "DELETE":
                            if (db == null || tb == null)
                                throw new Exception("not supported");
                            mess = Delete(tb,wh);
                            break;
                    }
                    Send(200, mess);
                } catch (Exception e)
                {
                    Send(403, e.Message);
                }
                conn?.Close();
            });
        }
        string Get1(string db)
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = (db==null)?"show databases":"show tables";
            var rdr = cmd.ExecuteReader();
            var r = "";
            while (rdr.Read())
                r += " " + rdr[0].ToString();
            return r;
        }
        string Get2(string tb, string wh)
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = "select * from " + tb;
            if (wh != null)
                cmd.CommandText += " where " + wh;
            var rdr = cmd.ExecuteReader();
            //  0: fieldstart 1: fieldend 2: liststart 3: listmid 4: listend 
            //  5: rowstart 6: rowmid 7: rowend                
            var fmt = new string[] { "","=","", "\n", "", "", ",", ";" };
            if (context.Request.Headers["Accept"] == "application/json")
                fmt = new string[] { "\"", "\": ", "[", ",\n", "]", "{", ", ", "}" };
            var r = new StringBuilder(fmt[2]);
            var m = "";
            while (rdr.Read())
            {
                var c = "";
                r.Append(m);
                m = fmt[3];
                r.Append(fmt[5]);
                for (var i = 0; i < rdr.VisibleFieldCount; i++)
                {
                    var sv = rdr[i].ToString();
                    r.Append(c+ fmt[0]+ rdr.GetName(i)+fmt[1]+ Format(rdr[i]));
                    c = fmt[6];
                }
                r.Append(fmt[7]);
            }
            return r + fmt[4];
        }
        string Post1(string data)
        {
            var tr = conn.BeginTransaction(System.Data.IsolationLevel.Serializable);
            foreach (var s in data.Split(';'))
            {
                var cmd = conn.CreateCommand();
                cmd.Transaction = tr;
                cmd.CommandText = s.Trim();
                if (cmd.CommandText == "")
                    continue;
                cmd.ExecuteNonQuery();
            }
            tr.Commit();
            return "OK";
        }
        string Post2(string tb,Document d)
        {
            var cls = "insert into " + tb + "(";
            var vls = ") values (";
            var cm = "";
            foreach (var e in d.fields)
            {
                cls += cm + e.Key;
                vls += cm + Format(e.Value);
                cm = ",";
            }
            var cmd = conn.CreateCommand();
            cmd.CommandText = cls + vls + ")";
            cmd.ExecuteNonQuery();
            return "OK";
        }
        string Put(string tb,string wh,Document d)
        {
            var cmt = "update " + tb + " set ";
            var cm = "";
            foreach (var e in d.fields)
            {
                cmt += cm + e.Key + "=" + Format(e.Value);
                cm = ",";
            }
            var cmd = conn.CreateCommand();
            cmd.CommandText = cmt+" where "+wh;
            cmd.ExecuteNonQuery();
            return "OK";
        }
        string Delete(string tb,string wh)
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = "delete from " + tb;
            if (wh != null)
                cmd.CommandText += " where " + wh;
            return "" + cmd.ExecuteNonQuery() + " rows deleted";
        }
        string Format(object ob)
        {
            if (ob is DBNull)
                return "null";
            if (ob is DateTime)
            {
                var dt = (DateTime)ob;
                return "'"+dt.ToString("yyyy-MM-dd")+"'";
            }
            if (ob is string)
                return "'" + ob.ToString() + "'";
            return ob.ToString();
        }
        string Receive()
        {
            var r = context.Request;
            if (!r.HasEntityBody)
                return "No data??";
            var rdr = new StreamReader(r.InputStream, r.ContentEncoding);
            var rs = rdr.ReadToEnd();
            r.InputStream.Close();
            rdr.Close();
            return rs;
        }
        void Send(int status,string mess)
        {
            var b = Encoding.UTF8.GetBytes(mess);
            var c = context.Response;
            c.StatusCode = status;
            c.StatusDescription = (status == 200) ? "OK" : "ERROR";
            c.AddHeader("Cache-control", "no-store");
            c.AddHeader("Expires", "-1");
            c.AddHeader("Pragma", "no-cache");
            c.ContentLength64 = b.Length;
            var st = c.OutputStream;
            st.Write(b, 0, b.Length);
            st.Close();
        }
    }
}
