using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.OleDb;
using System.Data.SQLite;
using System.IO;
using System.Runtime.ConstrainedExecution;


namespace ConverterMDB
{
    internal class Program
    {
        static int Main(string[] args)
        {
        
            string nameFile = "n180911";
            string conStrAccess = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=E:\\gate_db\\events\\n180911.mdb;Jet OLEDB:System Database=E:\\gate_db\\Gate.mdw;User ID=gate;Password=gate;";
            string conStrSQLite = "Data Source=E:\\gate_db\\converted\\n180911.sqlite;Version=3;";


            if (!File.Exists("E:\\gate_db\\converted\\n180911.sqlite"))
            {
                SQLiteConnection.CreateFile("E:\\gate_db\\converted\\n180911.sqlite");

                OleDbConnection connectAccess = new OleDbConnection(conStrAccess);
                SQLiteConnection connectSQLite = new SQLiteConnection(conStrSQLite);

                try
                {
                    connectSQLite.Open();
                    connectAccess.Open();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    Console.ReadKey();
                    return -1;
                }

                DataSet dataSet = new DataSet();
                string selectALL = "SELECT * FROM Events";
                OleDbDataAdapter adapter = new OleDbDataAdapter(selectALL, connectAccess);
                adapter.Fill(dataSet);
                connectAccess.Close();


                using (SQLiteCommand command = new SQLiteCommand(connectSQLite))
                {
                    command.CommandText = @"CREATE TABLE [Events] (
                    [Index] integer PRIMARY KEY AUTOINCREMENT NOT NULL,
                    [DateTime] char(100) NOT NULL,
                    [EventType] integer NOT NULL,
                    [EventCode] integer NOT NULL,
                    [DevPtr] integer NOT NULL,
                    [RdrPtr] integer NOT NULL,
                    [UserPtr] integer NOT NULL,
                    [OperatorID] integer NOT NULL,
                    [AlarmStatus] integer NOT NULL,
                    [Unit] char(100) NOT NULL,
                    [Message] char(100) NOT NULL,
                    [Name] char(100) NOT NULL
                    );"; // Поменять формат DateTime

                    command.CommandType = CommandType.Text;
                    command.ExecuteNonQuery();

                    string dataRowString = "";
                    foreach (DataRow row in dataSet.Tables[0].Rows)
                    {
                        dataRowString = "";

                        for (int i = 1; i < row.ItemArray.Length ; i++)
                        {
                            if (i != 1)
                            {
                                dataRowString += ", ";
                            }
                            switch (row.ItemArray[i].GetType().ToString())
                            {
                                case "System.Int32":
                                case "System.Byte":
                                    dataRowString += row.ItemArray[i].ToString();
                                    break;
                                case "System.String":
                                case "System.DateTime":
                                default:
                                    dataRowString += "\'" + row.ItemArray[i].ToString() + "\'";
                                    break;
                            } // Поменять формат DateTime
                        }

                        //Console.WriteLine(dataRowString);
                        command.CommandText = @"INSERT INTO [Events] 
                        ([DateTime],[EventType],[EventCode],[DevPtr],[RdrPtr],[UserPtr],[OperatorID],[AlarmStatus],[Unit],[Message],[Name]) 
                        VALUES (" + dataRowString + ")";

                        command.ExecuteNonQuery();
                    }
                }
                Console.WriteLine("Успешная конвертация...");
                Console.ReadKey();
            }

        return 0;

        }
    }
}
