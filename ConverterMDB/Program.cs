using System;
using System.Data;
using System.Data.OleDb;
using System.Data.SQLite;
using System.IO;
using System.Text.Json.Nodes;


namespace ConverterMDB
{
    internal class Program
    {
        public static void createTable(SQLiteConnection connectionSQLite, ref DataTable dataTable, string tableName)
        {
            using (SQLiteCommand command = new SQLiteCommand(connectionSQLite)) // Можно заменить string на stringBuilder
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
                        case "System.String":
                        default:
                            command.CommandText += " text";
                            break;
                    } // Поменять формат DateTime

                    if (i == 0) command.CommandText += " PRIMARY KEY AUTOINCREMENT NOT NULL, \n";
                    else if (i < dataTable.Columns.Count - 1) command.CommandText += ", \n";
                    else command.CommandText += ");";
                }
                //Console.WriteLine(command.CommandText+"\n");
                command.CommandType = CommandType.Text;
                command.ExecuteNonQuery(); // Автоматическое создание таблицы

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

                command.ExecuteNonQuery(); // Автоматическое заполнение таблицы
            }
        }

        static int Main(string[] args)
        {

            string filePath = "", fileName = "";
            string conStrAccess = "", conStrSQLite = "";
            OleDbConnection connectionAccess;
            SQLiteConnection connectionSQLite;


            string fileWorkgroupPath = "", login = "", password = "";

            if (File.Exists("ConnectionData.json"))
            {
                string jsonString = File.ReadAllText("ConnectionData.json");
                JsonObject jar = JsonNode.Parse(jsonString).AsObject(); ;
                fileWorkgroupPath = jar["fileWorkgroupPath"].ToString();
                login = jar["login"].ToString();
                password = jar["password"].ToString();
            }

            int index = 0;
            while (true) // Указать путь к файлу .mdb и проверить соединение //filePath = @"E:\gate_db\events\n180910.mdb";
            {
                Console.Write("Путь к файлу: \n > ");
                filePath = Console.ReadLine();

                if (filePath.Contains("\\") && filePath.Contains(".mdb"))
                {
                    conStrAccess = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + filePath +
                        ";Jet OLEDB:System Database=" + fileWorkgroupPath + ";User ID=" + login + ";Password=" + password + ";";
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
                    index = filePath.LastIndexOf("\\") + 1;
                    fileName = filePath.Substring(index, filePath.Length - index - 4);
                    filePath = filePath.Substring(0, index);
                    break;
                }
                else
                    Console.WriteLine("Указанный файл не .mdb\n");
            }

            if (!Directory.Exists(filePath + "converted"))
            {
                Directory.CreateDirectory(filePath + "converted");
            }

            if (!File.Exists(filePath + "converted\\" + fileName + ".sqlite")) // Создание базы данных
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
                OleDbDataAdapter adapter = new OleDbDataAdapter();
                string commSelectTable = "";
                const int tableNamesColumn = 2;
                int countConvertedTable = 0, countBlockedTable = 0, countSystemTable = 0;
                string tableName = "";

                Console.WriteLine("В файле " + fileName + " найдено " + schemeTable.Rows.Count + " таблиц(ы).");
                for (int i = 0; i < schemeTable.Rows.Count; i++) // Обработка таблиц
                {
                    tableName = schemeTable.Rows[i].ItemArray[tableNamesColumn].ToString();
                    commSelectTable = "SELECT * FROM " + tableName;

                    if (tableName.Contains("MSys")) // Пропуск системных таблиц
                    {
                        countSystemTable++;
                        continue;
                    }

                    try // Исключение - права доступа
                    {
                        adapter = new OleDbDataAdapter(commSelectTable, connectionAccess);
                        adapter.Fill(dataTable);
                    }
                    catch (Exception ex) // Можно сделать log-и
                    {
                        countBlockedTable++;
                        continue;
                    }

                    createTable(connectionSQLite, ref dataTable, tableName);
                    dataTable = new DataTable();
                    countConvertedTable++;
                }

                Console.WriteLine("Конвертировано - " + countConvertedTable + "\nНедоступно - " + countBlockedTable + "\nСистемные - " + countSystemTable);
                connectionSQLite.Close();
                connectionAccess.Close();
            }
            else { } // Обновление базы данных

            Console.ReadKey();
            return 0;
        }
    }
}