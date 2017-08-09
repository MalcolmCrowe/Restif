using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.Xml;
using System.Net;
using System.Web;
using System.IO;

namespace RESTClient
{
    public partial class ClientForm : Form
    {
        public ClientForm()
        {
            InitializeComponent();
        }

        string ContentType(string sel)
        {
            switch (sel)
            {
                case "SQL": return "text/plain";
                case "HTML": return "text/html";
                case "Json": return "application/json";
                case "String": return "text/plain";
            }
            return "application/xml";
        }

        void FormatResponse(Stream s)
        {
            switch (ReceiveBox.Text)
            {
                case "XML":
                    {
                        webBrowser1.Visible = false;
                        textBox2.Visible = true;
                        var ms = new MemoryStream();
                        var xw = new XmlTextWriter(ms, Encoding.UTF8);
                        var xd = new XmlDocument();
                        try
                        {
                            xd.LoadXml(new StreamReader(s).ReadToEnd());
                            xw.Formatting = Formatting.Indented;
                            xd.WriteContentTo(xw);
                            xw.Flush();
                            ms.Seek(0, SeekOrigin.Begin);
                            textBox2.Text= new StreamReader(ms).ReadToEnd();
                            break;
                        }
                        catch (Exception ex)
                        {
                            textBox2.Text = ex.ToString();
                        }
                        break;
                    }
                case "HTML":
                    {
                        webBrowser1.Visible = true;
                        textBox2.Visible = false;
                        try
                        {
                            webBrowser1.DocumentText = new StreamReader(s).ReadToEnd();
                        }
                        catch (Exception ex) 
                        {
                            statusLabel.Text = ex.Message;
                        }
                        break;
                    }
                default:
                    {
                        webBrowser1.Visible = false;
                        textBox2.Visible = true;
                        try
                        {
                            textBox2.Lines = new StreamReader(s).ReadToEnd().Split('\n');
                        }
                        catch (Exception ex)
                        {
                            statusLabel.Text = ex.Message;
                        }
                        break;
                    }
            }
        }

        private void Get_Click(object sender, EventArgs e)
        {
            statusLabel.Text = "";
            var rq = WebRequest.Create(textBox1.Text) as HttpWebRequest;
            if (Auth.Checked)
                rq.Credentials = new NetworkCredential(User.Text, Password.Text);
            else
                rq.UseDefaultCredentials = true;
            if (checkBox1.Checked)
                rq.Headers.Add("ETag", textBox3.Text);
            rq.Accept = ContentType(ReceiveBox.Text);
            var wr = GetResponse(rq);
            if (wr != null)
                textBox3.Text = wr.GetResponseHeader("ETag");
            if (wr != null && wr.ContentLength>0)
                FormatResponse(wr.GetResponseStream());
            else
            {
                webBrowser1.DocumentText = "";
                textBox2.Text = "";
            }
        }

        private void Put_Click(object sender, EventArgs e)
        {
            statusLabel.Text = "";
            var rq = WebRequest.Create(textBox1.Text) as HttpWebRequest;
            if (checkBox1.Checked)
                rq.Headers.Add("ETag", textBox3.Text);
            if (Auth.Checked)
                rq.Credentials = new NetworkCredential(User.Text, Password.Text);
            else
                rq.UseDefaultCredentials = true;
            rq.ContentType = ContentType(SendBox.Text);
            rq.Method = "PUT";
            SendData(rq);
            var wr = GetResponse(rq);
            if (wr != null)
                textBox3.Text = wr.GetResponseHeader("ETag");
            if (wr != null && wr.StatusCode == HttpStatusCode.PreconditionFailed)
            {
                webBrowser1.Visible = false;
                textBox2.Visible = true;
                textBox2.Text = new StreamReader(wr.GetResponseStream()).ReadToEnd();
            }
        }

        private void Delete_Click(object sender, EventArgs e)
        {
            statusLabel.Text = "";
            var rq = WebRequest.Create(textBox1.Text) as HttpWebRequest;
            if (checkBox1.Checked)
                rq.Headers.Add("ETag", textBox3.Text);
            if (Auth.Checked)
                rq.Credentials = new NetworkCredential(User.Text, Password.Text);
            else
                rq.UseDefaultCredentials = true;
            rq.Method = "DELETE";
            var wr = GetResponse(rq);
            if (wr!=null)
                textBox3.Text = wr.GetResponseHeader("ETag");
            if (wr != null && wr.StatusCode == HttpStatusCode.PreconditionFailed)
            {
                webBrowser1.Visible = false;
                textBox2.Visible = true;
                textBox2.Text = new StreamReader(wr.GetResponseStream()).ReadToEnd();
            }
        }

        private void Post_Click(object sender, EventArgs e)
        {
            statusLabel.Text = "";
            var rq = WebRequest.Create(textBox1.Text) as HttpWebRequest;
            if (checkBox1.Checked)
                rq.Headers.Add("If-Match", textBox3.Text);
            if (Auth.Checked)
                rq.Credentials = new NetworkCredential(User.Text, Password.Text);
            else
                rq.UseDefaultCredentials = true;
            rq.ContentType = ContentType(SendBox.Text);
            rq.Method = "POST";
            SendData(rq);
            var wr = GetResponse(rq);
            if (wr != null && wr.StatusCode == HttpStatusCode.OK)
            {
                textBox3.Text = wr.GetResponseHeader("ETag");
                var ct = wr.GetResponseHeader("Content-Type");
                if (ct!="" && ct!="text/plain")
                    FormatResponse(wr.GetResponseStream());
            }
            if (wr != null && wr.StatusCode != HttpStatusCode.OK)
            {
                webBrowser1.Visible = false;
                statusLabel.Text += ": "+new StreamReader(wr.GetResponseStream()).ReadToEnd();
            }
        }

        void SendData(WebRequest rq)
        {
            var rst = new StreamWriter(rq.GetRequestStream());
            rst.WriteLine(textBox2.Text);
            rst.Close();
        }

        HttpWebResponse GetResponse(WebRequest rq)
        {
            HttpWebResponse wr = null;
            try
            {
                wr = rq.GetResponse() as HttpWebResponse;
            }
            catch (WebException e)
            {
                statusLabel.Text = e.Message;
                wr = e.Response as HttpWebResponse;
            }
            catch (Exception e)
            {
                statusLabel.Text = e.Message;
            }
            if (wr!=null)
                statusLabel.Text = wr.StatusDescription;
            return wr;
        }

        private void SendBox_SelectedIndexChanged(object sender, EventArgs e)
        {
            statusLabel.Text = "";
            webBrowser1.Visible = false;
            textBox2.Visible = true;
        }

        private void ReceiveBox_SelectedIndexChanged(object sender, EventArgs e)
        {
            statusLabel.Text = "";
            webBrowser1.Visible = false;
            textBox2.Visible = true;
        }
    }
}