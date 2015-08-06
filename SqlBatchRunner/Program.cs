using System;
using System.IO;
using System.Configuration;

namespace SqlBatchRunner
{
    class Program
    {
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Globalization", "CA1303:Do not pass literals as localized parameters", MessageId = "System.Console.WriteLine(System.String,System.Object)")]
        static int Main(string[] args)
        {
            var result = 0;

            //ConfigScanner.GenerateSampleConfig("test.config");

            if (args.Length == 1)
            {
                /**
                 * This will run *.sql files found in directory passed in via command line.
                 */

                //  create SqlBatchControl table if it doesn't exist
                //SqlRunner.createControlTable();

                //  execute sql found in target folder
                Console.WriteLine("Executing SQL found in {0}", args[0]);
                //SqlRunner.Run(args[0]);
            }
            else if (args.Length == 0)
            {
                //  If no command line arguments, first check the directory in the App.Config.
                //  Otherwise, just execute the *.sql found in the current directory.
                String directoryName = ConfigurationManager.AppSettings["DirectoryName"] != "" ? ConfigurationManager.AppSettings["DirectoryName"] : Environment.CurrentDirectory;
                String connectionString = ConfigurationManager.AppSettings["ConnectionString"];

                //  create SqlBatchControl table if it doesn't exist
                var runner = new SqlRunner(connectionString);

                //  execute sql found folder local to execution context
                Console.WriteLine("Executing SQL found in {0}", directoryName);
                runner.Run(directoryName);
            }
            else if (args.Length >= 2)
            {
                var directoryPath = args[0];
                var xmlFile = args[1];

                var c = new ConfigScanner(xmlFile);

                //foreach (var a in args)
                //{
                //    if (a.StartsWith("-"))
                //    {
                //        if (a == "-m")
                //        {
                //            Console.WriteLine("Manual mode enabled");
                //            c.EnableManualMode();
                //        }
                //    }
                //}

                if (args.Length == 3 && args[2] == "-m")
                {
                    Console.WriteLine("Manual mode enabled");
                    c.EnableManualMode();
                }

                try
                {
                    if (File.Exists(directoryPath))
                    {
                        var directory = Path.GetDirectoryName(args[0]);
                        if (!c.ProcessFile(directory, args[0]))
                        {
                            Console.WriteLine("No configuration file found.");
                            return 1;
                        }
                    }
                    else
                    {
                        if (!c.ProcessDirectory(directoryPath))
                        {
                            Console.WriteLine("No configuration file found.");
                            return 1;
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("Execution failure: {0}", e.Message);
                    return 1;
                }
            }
            else
            {
                Console.WriteLine("Expecting only one argument, the path to sql files.");
            }

            return result;
        }
    }
}

//manual mode.
//force single file