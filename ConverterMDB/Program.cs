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
using System.Data.Entity.Core.Common.CommandTrees.ExpressionBuilder;


namespace ConverterMDB
{
    internal class Program
    {
        public static void createTable(SQLiteConnection connectionSQLite, ref DataTable dataTable, string tableName)
        {
            using (SQLiteCommand command = new SQLiteCommand(connectionSQLite))
            {
                command.CommandText = "CREATE TABLE [" + tableName + "] (\n";
                for (int i = 0; i < dataTable.Columns.Count; i++)
                {
                    command.CommandText += "[" + dataTable.Columns[i].ColumnName + "]";
                    switch (dataTable.Rows[0].ItemArray[i].GetType().ToString())
                    {
                        case "System.Int32":
                        case "System.Byte":
                            command.CommandText += " integer";
                            break;
                        case "System.DateTime":
                            //command.CommandText += " char (50)";
                            //break;
                        case "System.String":
                        default:
                            command.CommandText += " text";
                            break;
                    } // Поменять формат DateTime

                    if (i == 0) command.CommandText += " PRIMARY KEY AUTOINCREMENT NOT NULL, \n";
                    else if (i < dataTable.Columns.Count-1) command.CommandText += ", \n";
                    else command.CommandText += ");";
                }
                Console.WriteLine(command.CommandText+"\n");
                command.CommandType = CommandType.Text;
                command.ExecuteNonQuery();

                string dataRowString = "", dataListString = "";
                int num = 0;
                foreach (DataRow row in dataTable.Rows)
                {
                    dataRowString = "";
                    num++;

                    for (int i = 1; i < row.ItemArray.Length; i++)
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
                    dataListString += "(" + dataRowString + ")";
                    if (num < dataTable.Rows.Count)
                        dataListString += ", \n";

                }

                command.CommandText = "INSERT INTO [" + tableName + "] \n(";
                for (int i = 1; i < dataTable.Columns.Count; i++)
                {
                    command.CommandText += "[" + dataTable.Columns[i].ColumnName + "]";
                    if (i < dataTable.Columns.Count - 1) command.CommandText += ", ";
                }
                command.CommandText += ")\nVALUES " + dataListString + "; ";
                //Console.WriteLine(command.CommandText);

                command.ExecuteNonQuery();
            }
        }

        static int Main(string[] args)
        {
            string filePath = "", fileName = "";
            string conStrAccess = "", conStrSQLite = "";
            OleDbConnection connectionAccess;
            SQLiteConnection connectionSQLite;

            int index = 0;
            while (true) // Указать путь к файлу .mdb и проверить соединение
            {
                Console.WriteLine("Path to file:");
                Console.Write(" > ");
                filePath = Console.ReadLine();
                //filePath = @"E:\gate_db\events\n180910.mdb";

                if (filePath.Contains("\\") && filePath.Contains(".mdb"))
                {
                    conStrAccess = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + filePath + ";" +
                        "Jet OLEDB:System Database=E:\\gate_db\\Gate.mdw;User ID=gate;Password=gate;";
                    index = filePath.LastIndexOf("\\") + 1;
                    connectionAccess = new OleDbConnection(conStrAccess);
                    try
                    {
                        connectionAccess.Open();
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                        continue;
                    }
                    fileName = filePath.Substring(index, filePath.Length - index - 4);
                    filePath = filePath.Substring(0, index);
                    break;
                }
                else
                {
                    filePath = "";
                    fileName = "";
                    Console.WriteLine("Not a file .mdb");
                }
            }

            if (!Directory.Exists(filePath + "converted"))
            {
                Directory.CreateDirectory(filePath + "converted");
            }

            if (!File.Exists(filePath + "converted\\" + fileName + ".sqlite")) // Create DB
            {
                SQLiteConnection.CreateFile(filePath + "converted\\" + fileName + ".sqlite");
                conStrSQLite = "Data Source=" + filePath + "converted\\" + fileName + ".sqlite;Version=3;";
                connectionSQLite = new SQLiteConnection(conStrSQLite);

                try
                {
                    connectionSQLite.Open();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    Console.ReadKey();
                    connectionAccess.Close();
                    return -1;
                }

                DataTable dataTable = new DataTable();
                DataTable schemeTable = connectionAccess.GetSchema("Tables");
                string commSelectTable = "";
                const int tableNamesColumn = 2;
                OleDbDataAdapter adapter = new OleDbDataAdapter();

                Console.WriteLine("В файле " + fileName + " найдено " + schemeTable.Rows.Count + " таблиц(ы).");
                int countConvertedTable = 0, countBlockedTable = 0, countSystemTable = 0;
                string tableName = "";
                for (int i = 0; i < schemeTable.Rows.Count; i++) // Считывание и проверка доступа к таблицам с данными
                {
                    tableName = schemeTable.Rows[i].ItemArray[tableNamesColumn].ToString();
                    commSelectTable = "SELECT * FROM " + tableName;
                    if (tableName.Contains("MSys"))
                    {
                        countSystemTable++;
                        continue;
                    }
                    try
                    {
                        adapter = new OleDbDataAdapter(commSelectTable, connectionAccess);
                        dataTable = new DataTable();
                        adapter.Fill(dataTable);
                    }
                    catch (Exception ex)
                    {
                        countBlockedTable++;
                        continue;
                    }

                    createTable(connectionSQLite, ref dataTable, tableName);
                    countConvertedTable++;
                }

                Console.WriteLine("Успешно конвертировано - " + countConvertedTable + "\nНедоступно или с ошибкой - " + countBlockedTable + "\nПропущено системных - " + countSystemTable);
                connectionSQLite.Close();
                connectionAccess.Close();
            }
            else { } // Update DB

            Console.ReadKey();
            return 0;

        }
    }
}
