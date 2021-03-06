using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NLog.Web;
using NLog.Config;
using System.Runtime.InteropServices;
using SendGrid.Extensions.DependencyInjection;
using System.Reflection;

namespace TequaCreek.BloxGuardianMessageProcessingService
{
    public class Program
    {
        public static void Main(string[] args)
        {

            //var logger = NLog.Web.NLogBuilder.ConfigureNLog("nlog.config").GetCurrentClassLogger();
            var environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
            var isProd = environment == Microsoft.Extensions.Hosting.Environments.Production;
            var logger = NLogBuilder.ConfigureNLog("nlog.config").GetCurrentClassLogger();

            try
            {

                logger.Info("Starting BloxGuardian Message Processing Service");
                logger.Info("Assembly version {0}", Assembly.GetEntryAssembly().GetName().Version);
                logger.Info("File version {0}", Assembly.GetEntryAssembly().GetCustomAttribute<AssemblyFileVersionAttribute>().Version);
                logger.Info("Product version {0}", Assembly.GetEntryAssembly().GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion);

                CreateHostBuilder(args).Build().Run();
            }
            catch (Exception ex)
            {
                //NLog: catch setup errors
                logger.Error(ex, "Stopped program because of exception");
                throw;
            }
            finally
            {
                // Ensure to flush and stop internal timers/threads before application-exit (Avoid segmentation fault on Linux)
                NLog.LogManager.Shutdown();
            }

        }

        public static IHostBuilder CreateHostBuilder(string[] args) {

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                return Host.CreateDefaultBuilder(args)
                    .UseSystemd()                
                    .ConfigureServices((hostContext, services) =>
                    {
                        services.AddHostedService<Worker>();
                        services.AddSendGrid(options => { options.ApiKey = Environment.GetEnvironmentVariable("SENDGRID_API_KEY"); });
                    })
                    .UseNLog();
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
               return Host.CreateDefaultBuilder(args)
                    .UseWindowsService()
                    .ConfigureServices((hostContext, services) =>
                    {
                        services.AddHostedService<Worker>();
                        services.AddSendGrid(options => { options.ApiKey = Environment.GetEnvironmentVariable("SENDGRID_API_KEY"); });
                    })
                    .UseNLog();

            } else
            {
                return null;
            }

        }       // CreateHostBuilder()


    }
}
