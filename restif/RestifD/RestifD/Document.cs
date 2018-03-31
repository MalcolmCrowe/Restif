using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace RestifD
{
    /// <summary>
    /// The Document class here represents JavaScript style objects.
    /// The constructor and ToString() support JSon serialisation.
    /// There are methods using reflection to create a Document representing
    /// any CLI object of a known type, 
    /// and for extracting objects from a Document given a path of fieldnames.
    /// See also DocArray,
    /// </summary>
    public class Document : DocBase
    {
        public List<KeyValuePair<string, object>> fields = new List<KeyValuePair<string, object>>();
        public Document()
        { }
        public Document(string s)
        {
            if (s == null)
                return;
            s = s.Trim();
            int n = s.Length;
            if (n == 0 || s[0] != '{')
                throw new DocumentException("{ expected");
            var i = Fields(s, 1, n);
            if (i != n)
                throw new DocumentException("unparsed input at " + (i - 1));
        }
        public object this[string k]
        {
            get
            {
                foreach (var p in fields)
                    if (p.Key == k)
                        return p.Value;
                return null;
            }
        }
        public bool Contains(string k)
        {
            foreach (var p in fields)
                if (p.Key == k)
                    return true;
            return false;
        }
        private enum ParseState { StartKey, Key, Colon, StartValue, Comma }
        /// <summary>
        /// Parse the contents of {} 
        /// </summary>
        /// <param name="s">the string</param>
        /// <param name="i">the start of the fields</param>
        /// <param name="n">the end of the string</param>
        /// <returns>the position just after the }</returns>
        internal int Fields(string s, int i, int n)
        {
            ParseState state = ParseState.StartKey;
            StringBuilder kb = null;
            while (i < n)
            {
                var c = s[i++];
                switch (state)
                {
                    case ParseState.StartKey:
                        kb = new StringBuilder();
                        if (Char.IsWhiteSpace(c))
                            continue;
                        if (c == '}' && fields.Count == 0)
                            return i;
                        if (c != '"')
                            throw new DocumentException("Expected \" at " + (i - 1));
                        state = ParseState.Key;
                        continue;
                    case ParseState.Key:
                        if (c == '"')
                        {
                            state = ParseState.Colon;
                            continue;
                        }
                        if (c == '\\')
                            c = GetEscape(s, n, ref i);
                        kb.Append(c);
                        continue;
                    case ParseState.Colon:
                        if (Char.IsWhiteSpace(c))
                            continue;
                        if (c != ':')
                            throw new DocumentException("Expected : at " + (i - 1));
                        state = ParseState.StartValue;
                        continue;
                    case ParseState.StartValue:
                        if (Char.IsWhiteSpace(c))
                            continue;
                        fields.Add(new KeyValuePair<string, object>
                                (kb.ToString(), GetValue(s, n, ref i)));
                        state = ParseState.Comma;
                        continue;
                    case ParseState.Comma:
                        if (Char.IsWhiteSpace(c))
                            continue;
                        if (c == '}')
                            return i;
                        if (c != ',')
                            throw new DocumentException("Expected , at " + (i - 1));
                        state = ParseState.StartKey;
                        continue;
                }
            }
            throw new DocumentException("Incomplete syntax at " + (i - 1));
        }
        public static void Field(object v, StringBuilder sb)
        {
            if (v == null || v is DBNull)
                sb.Append("null");
            else if (v is string)
            {
                sb.Append('\'');
                sb.Append(v);
                sb.Append('\'');
            }
            else if (v is IEnumerable)
            {
                var comma = "";
                sb.Append("[");
                foreach (var a in (IEnumerable)v)
                {
                    sb.Append(comma);
                    comma = ",";
                    if (a is Document)
                        sb.Append(((Document)a).ToString());
                    else if (a is DocArray)
                        sb.Append(((DocArray)a).ToString());
                    else if (a is DateTime)
                        sb.Append((((DateTime)a).Ticks*1.0D/TimeSpan.TicksPerSecond).ToString());
                    else if (a is string)
                        sb.Append("'" + a + "'");
                    else
                        sb.Append(a.ToString());
                }
                sb.Append("]");
            }
            else
                sb.Append(v.ToString());
        }
        public override string ToString()
        {
            var sb = new StringBuilder("{");
            var comma = "";
            foreach (var f in fields)
            {
                sb.Append(comma); comma = ", ";
                sb.Append('"');
                sb.Append(f.Key);
                sb.Append("\": ");
                Field(f.Value, sb);
            }
            sb.Append("}");
            return sb.ToString();
        }
        internal static int GetLength(byte[] b, int off)
        {
            return b[off] + (b[off + 1] << 8) + (b[off + 2] << 16) + (b[off + 3] << 24);
        }

        internal static void Format(Document e, StringBuilder cls, StringBuilder vls)
        {
            var comma = "";
            foreach (var f in e.fields)
            {
                cls?.Append(comma);
                vls.Append(comma);
                comma = ", ";
                cls?.Append(f.Key);
                Field(f.Value, vls);
            }
        }
    }
    public class DocArray : DocBase
    {
        public List<object> items = new List<object>();
        public DocArray() { }
        public DocArray(string s)
        {
            if (s == null)
                return;
            s = s.Trim();
            int n = s.Length;
            if (n <= 2 || s[0] != '[' || s[n - 1] != ']')
                throw new DocumentException("[..] expected");
            int i = Items(s, 1, n);
            if (i != n)
                throw new DocumentException("bad DocArray format");
        }
        private enum ParseState { StartValue, Comma }
        internal int Items(string s, int i, int n)
        {
            var state = ParseState.StartValue;
            while (i < n)
            {
                var c = s[i++];
                if (Char.IsWhiteSpace(c))
                    continue;
                if (c == ']' && items.Count == 0)
                    break;
                switch (state)
                {
                    case ParseState.StartValue:
                        items.Add(GetValue(s, n, ref i));
                        state = ParseState.Comma;
                        continue;
                    case ParseState.Comma:
                        if (c == ']')
                            return i;
                        if (c != ',')
                            throw new DocumentException(", expected");
                        state = ParseState.StartValue;
                        continue;
                }
            }
            return i;
        }
        public override string ToString()
        {
            var sb = new StringBuilder("[");
            var comma = "";
            foreach (var e in items)
            {
                sb.Append(comma);
                sb.Append(e.ToString());
                comma = ",";
            }
            sb.Append("]");
            return sb.ToString();
        }
   }
    public class DocumentException : Exception
    {
        public DocumentException(string msg) : base(msg) { }
    }
    public class DocBase
    {
        protected DocBase() { }
        protected object GetValue(string s, int n, ref int i)
        {
            if (i < n)
            {
                var c = s[i - 1];
                if (c == '"' || c=='\'')
                    return GetString(s, n, c, ref i);
                if (c == '{')
                {
                    var d = new Document();
                    i = d.Fields(s, i, n);
                    return d;
                }
                if (c == '[')
                {
                    var d = new DocArray();
                    i = d.Items(s, i, n);
                    return d;
                }
                if (i + 3 < n && s.Substring(i - 1, 4) == "true")
                {
                    i += 3;
                    return true;
                }
                if (i + 4 < n && s.Substring(i - 1, 5) == "false")
                {
                    i += 4;
                    return false;
                }
                if (i + 3 < n && s.Substring(i - 1, 4) == "null")
                {
                    i += 3;
                    return null;
                }
                var sg = c == '-';
                if (sg && i < n)
                    c = s[i++];
                var whole = 0L;
                if (Char.IsDigit(c))
                {
                    i--;
                    whole = GetHex(s, n, ref i);
                    while (i < n && Char.IsDigit(s[i]))
                        whole = whole * 10 + GetHex(s, n, ref i);
                }
                else
                    goto bad;
                if (i >= n || (s[i] != '.' && s[i] != 'e' && s[i] != 'E'))
                    return sg ? -whole : whole;
                int scale = 0;
                if (s[i] == '.')
                {
                    if (++i >= n || !Char.IsDigit(s[i]))
                        throw new DocumentException("decimal part expected");
                    while (i < n && Char.IsDigit(s[i]))
                    {
                        whole = whole * 10 + GetHex(s, n, ref i);
                        scale++;
                    }
                }
                if (i >= n || (s[i] != 'e' && s[i] != 'E'))
                {
                    var m = (decimal)whole;
                    while (scale-- > 0)
                        m /= 10M;
                    return sg ? -m : m;
                }
                if (++i >= n)
                    throw new DocumentException("exponent part expected");
                var esg = s[i] == '-';
                if ((s[i] == '-' || s[i] == '+') && (++i >= n || !Char.IsDigit(s[i])))
                    throw new DocumentException("exponent part expected");
                var exp = 0;
                while (i < n && Char.IsDigit(s[i]))
                    exp = exp * 10 + GetHex(s, n, ref i);
                if (esg)
                    exp = -exp;
                var dr = whole * Math.Pow(10.0, exp - scale);
                return sg ? -dr : dr;
            }
            bad:
            throw new DocumentException("Value expected at " + (i - 1));
        }
        protected string GetString(string s, int n, char q, ref int i)
        {
            var sb = new StringBuilder();
            while (i < n)
            {
                var c = s[i++];
                if (c == q)
                    return sb.ToString();
                if (c == '\\')
                    c = GetEscape(s, n, ref i);
                sb.Append(c);
            }
            throw new DocumentException("Non-terminated string at " + (i - 1));
        }
        protected char GetEscape(string s, int n, ref int i)
        {
            if (i < n)
            {
                var c = s[i++];
                switch (c)
                {
                    case '"': return c;
                    case '\\': return c;
                    case '/': return c;
                    case 'b': return '\b';
                    case 'f': return '\f';
                    case 'n': return '\n';
                    case 'r': return '\r';
                    case 't': return '\t';
                    case 'u':
                        {
                            int v = 0;
                            for (int j = 0; j < 4; i++)
                                v = (v << 4) + GetHex(s, n, ref i);
                            return (char)v;
                        }
                    case 'U': goto case 'u';
                }
            }
            throw new DocumentException("Illegal escape");
        }
        internal static int GetHex(string s, int n, ref int i)
        {
            if (i < n)
            {
                switch (s[i++])
                {
                    case '0': return 0;
                    case '1': return 1;
                    case '2': return 2;
                    case '3': return 3;
                    case '4': return 4;
                    case '5': return 5;
                    case '6': return 6;
                    case '7': return 7;
                    case '8': return 8;
                    case '9': return 9;
                    case 'a': return 10;
                    case 'b': return 11;
                    case 'c': return 12;
                    case 'd': return 13;
                    case 'e': return 14;
                    case 'f': return 15;
                    case 'A': return 10;
                    case 'B': return 11;
                    case 'C': return 12;
                    case 'D': return 13;
                    case 'E': return 14;
                    case 'F': return 15;
                }
            }
            throw new DocumentException("Hex digit expected at " + (i - 1));
        }

    }

}
