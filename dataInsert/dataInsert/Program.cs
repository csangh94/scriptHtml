using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Dynamic;

namespace dataInsert
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Start");
            string dateTime = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            var strFolerName = args.Length == 0 ? "C:\\Users\\tqoon-83\\Desktop\\lsh\\ATAM\\html\\" : args[0];
            DirectoryInfo dirInfo = new DirectoryInfo(strFolerName);
            string totalUrl, targetUrl, contents, fileDropDate;
            Console.WriteLine("=========================DB Connection 시작=====================");
            try
            {

                foreach (FileInfo fileInfo in dirInfo.GetFiles())
                {

                    string FileName = fileInfo.Name;
                    string path = fileInfo.FullName;
                    string textValue = File.ReadAllText(path, Encoding.UTF8);
                    string[] fileNameSplit = FileName.Split('_');
                    
                    totalUrl = GetTotalUrl(fileNameSplit); 
                    targetUrl = fileNameSplit[0];

                    fileDropDate = GetFileDropDate(fileNameSplit);// 20200924153542.txt >> 20200924153542

                    contents = textValue;

                    var fileDate = GetId(totalUrl);
                    int id = fileDate.id;
                    long oldDate = long.Parse(fileDate.fileDropDate);
                    long newDate = long.Parse(fileDropDate);
                    Console.WriteLine(id);
                    if (id == 0)
                    {
                        Console.WriteLine("===============INSERT=============");
                        Insert(totalUrl, targetUrl, fileDropDate, contents, dateTime);
                    }
                    else if(id > 0 && newDate > oldDate)
                    {
                        Console.WriteLine("===============UPDATE=============");
                        Update(id, fileDropDate, contents, dateTime);
                    }
                    else
                    {
                        Console.WriteLine("===============PASS=============");
                    }
                }
                Console.WriteLine("=========================Connection 종료=====================");


            }
            catch (Exception e)
            {
                Console.WriteLine("Connection 실패" + e);
            }
            Console.ReadLine();
        }

        // FileName에서 날짜만 추출
        public static string GetFileDropDate(string[] fileNameSplit)
        {
            string dataFullName = fileNameSplit[fileNameSplit.Count() - 1];
            string date = dataFullName.Split('.')[0];
            return date;
        }

        // FileName에서 날짜를 제외 
        public static string GetTotalUrl(string[] fileNameSplit)
        {
            string totalUrl = fileNameSplit[0];
            for (int i = 1; i < fileNameSplit.Count() - 1; i++)
            {
                totalUrl = totalUrl + "/" + fileNameSplit[i];
            }
            return totalUrl;
        }

        //이미 입력된 사이트인지 구분
        public static dynamic GetId(string totalUrl)
        {
            SqlConnection sqlConn = new SqlConnection("Server=192.168.1.62, 1433; Database=adprintNewDB; uid=sa; pwd=Y^,30Xdf");
            sqlConn.Open();
            SqlCommand sqlComm = new SqlCommand();
            sqlComm.Connection = sqlConn;
            sqlComm.CommandText = "SELECT * FROM ElasticInsert WHERE totalUrl=@totalUrl";
            sqlComm.Parameters.AddWithValue("@totalUrl", totalUrl);
            SqlDataReader rdr = sqlComm.ExecuteReader();
            dynamic tt = new ExpandoObject();
            if (rdr.Read() == true)
            {
                tt.fileDropDate = (string)rdr["fileDropDate"];
                tt.id = (int)rdr["id"];
                sqlConn.Close();
            }
            else
            {
                tt.fileDropDate ="0";
                tt.id = 0;
                sqlConn.Close();
            }
            return tt;


        }

        // 데이터 입력
        public static void Insert(string totalUrl, string targetUrl,string fileDropDate, string contents, string dateTime)
        {
            SqlConnection sqlConn = new SqlConnection("Server=192.168.1.62, 1433; Database=adprintNewDB; uid=sa; pwd=Y^,30Xdf");
            sqlConn.Open();
            SqlCommand sqlComm = new SqlCommand();
            sqlComm.Connection = sqlConn;
            sqlComm.CommandText = "INSERT into ElasticInsert (totalUrl, targetUrl,fileDropDate ,contents, lastUpdateTime) values (@totalUrl,@targetUrl,@fileDropDate, @contents, @lastUpdateTime)";
            sqlComm.Parameters.AddWithValue("@totalUrl", totalUrl);
            sqlComm.Parameters.AddWithValue("@targetUrl", targetUrl);
            sqlComm.Parameters.AddWithValue("@fileDropDate", fileDropDate);
            sqlComm.Parameters.AddWithValue("@contents", contents);
            sqlComm.Parameters.AddWithValue("@lastUpdateTime", dateTime);
            sqlComm.ExecuteNonQuery();
            sqlConn.Close();
        }

        // 데이터 수정
        public static void Update(int id,string fileDropDate, string contents, string dateTime)
        {
            SqlConnection sqlConn = new SqlConnection("Server=192.168.1.62, 1433; Database=adprintNewDB; uid=sa; pwd=Y^,30Xdf");
            SqlCommand sqlComm = new SqlCommand();
            sqlComm.Connection = sqlConn;
            sqlComm.CommandText = "UPDATE ElasticInsert SET contents=@contents,lastUpdateTime=@lastUpdateTime, fileDropDate=@fileDropDate WHERE id=@id ";
            sqlComm.Parameters.AddWithValue("@id", id);
            sqlComm.Parameters.AddWithValue("@fileDropDate", fileDropDate);
            sqlComm.Parameters.AddWithValue("@contents", contents);
            sqlComm.Parameters.AddWithValue("@lastUpdateTime", dateTime);
            sqlConn.Open();
            sqlComm.ExecuteNonQuery();
            sqlConn.Close();
        }

        

    }
    
}
