using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using System;
using System.Reflection;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using Newtonsoft.Json;
using Npgsql;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Net;
using System.Text;

namespace TequaCreek.BloxGuardianMessageProcessingService
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> logger;
        private IConfiguration configuration;

        private Timer timer = null!;

        private List<RabbitMQ.Client.Events.AsyncEventingBasicConsumer> consumers;
        private List<RabbitMQ.Client.IModel> channels;
        private List<RabbitMQ.Client.IConnection> connections;

        private RabbitMQ.Client.ConnectionFactory rmqFromBloxChannelConnectionFactory;
        private RabbitMQ.Client.IConnection rmqFromBloxChannelConnection;
        private RabbitMQ.Client.IModel rmqFromBloxChannelChannel;
        private RabbitMQ.Client.Events.AsyncEventingBasicConsumer rmqFromBloxChannelConsumer;
        private RabbitMQ.Client.ConnectionFactory rmqToBloxChannelConnectionFactory;
        private RabbitMQ.Client.IConnection rmqToBloxChannelConnection;
        private RabbitMQ.Client.IModel rmqToBloxChannelChannel;
        private RabbitMQ.Client.Events.AsyncEventingBasicConsumer rmqToBloxChannelConsumer;

        public string processingServerId { get; set; }

        public Worker(IConfiguration configuration, ILogger<Worker> logger)
        {
            this.logger = logger;
            this.configuration = configuration;

            this.processingServerId = configuration["AppSettings:ProcessingServerId"];

            consumers = new List<AsyncEventingBasicConsumer>();
            channels = new List<IModel>();
            connections = new List<IConnection>();

        }

        public override async Task StartAsync(CancellationToken cancellationToken)
        {

            string exchangeName;
            string queueName;

            try
            {

                logger.LogInformation("Starting BloxGuardian Message Processing Service (Assigned Processing Server ID = {0})", this.processingServerId);
                logger.LogInformation("Assembly version {0}", Assembly.GetEntryAssembly().GetName().Version);
                logger.LogInformation("File version {0}", Assembly.GetEntryAssembly().GetCustomAttribute<AssemblyFileVersionAttribute>().Version);
                logger.LogInformation("Product version {0}", Assembly.GetEntryAssembly().GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion);

                logger.LogInformation("Setting up Production messaging queue handling");
                logger.LogInformation("Opening production connection to From-BloxChannel message queue host");
                logger.LogInformation("  -- Host Name = {0}, Port = {1}, Virtual Host = {0}",
                                        configuration["AppSettings:FromBloxChannelMessageQueueHostName"],
                                        configuration["AppSettings:FromBloxChannelMessageQueuePort"],
                                        configuration["AppSettings:FromBloxChannelMessageQueueVirtualHostName"]);
                rmqFromBloxChannelConnectionFactory = new RabbitMQ.Client.ConnectionFactory()
                {
                    HostName = configuration["AppSettings:FromBloxChannelMessageQueueHostName"],
                    Port = int.Parse(configuration["AppSettings:FromBloxChannelMessageQueuePort"]),
                    VirtualHost = configuration["AppSettings:FromBloxChannelMessageQueueVirtualHostName"],
                    UserName = configuration["AppSettings:FromBloxChannelMessageQueueUserName"],
                    Password = configuration["AppSettings:FromBloxChannelMessageQueueUserPassword"],
                    DispatchConsumersAsync = true
                };

                rmqFromBloxChannelConnection = rmqFromBloxChannelConnectionFactory.CreateConnection();
                rmqFromBloxChannelChannel = rmqFromBloxChannelConnection.CreateModel();

                exchangeName = configuration["AppSettings:FromBloxChannelMessageQueueExchangeName"];

                // PROGRAMMER'S NOTE:  Best practice is to just declare definitions.  If Exchange and Queue already exist, the functions
                //                     simply return without action.
                rmqFromBloxChannelChannel.ExchangeDeclare(exchangeName, RabbitMQ.Client.ExchangeType.Direct, true, false, null);

                /////////////////////////////////////////
                /// From BloxChannel messages channel ///
                /////////////////////////////////////////

                queueName = string.Format(TequaCreek.BloxChannelDataModelLibrary.SharedConstantValues.MESSAGE_QUEUE_NAME_PATTERN_BLOXCHANNEL_TO_BLOXGUARDIAN,
                                            this.processingServerId);
                logger.LogInformation("Setting up From-BloxGuardian queue {0} definition (if not already exists)", queueName);

                Dictionary<string, object> args = new Dictionary<string, object>();

                // PROGRAMMER'S NOTE:  Add any queue definition arguments here (NOTE:  this may cause a conflict if queue already defined
                //                                                                     with different args)

                rmqFromBloxChannelChannel.QueueDeclare(queueName, true, false, false, args);

                logger.LogInformation("Attaching message handler Worker::FromBloxChannelMessageReceivedAsync() to To-BloxGuardian queue {0}", queueName);

                rmqFromBloxChannelConsumer = new RabbitMQ.Client.Events.AsyncEventingBasicConsumer(rmqFromBloxChannelChannel);
                rmqFromBloxChannelConsumer.Received += FromBloxChannelMessageReceivedAsync;

                // PROGRAMMER'S NOTE:  May want to add other event handlers here for shutdown, etc.

                rmqFromBloxChannelChannel.BasicConsume(queueName, false, "", false, false, null, rmqFromBloxChannelConsumer);


                logger.LogInformation("Setting up timed activities handler to fire every {0} seconds", configuration["AppSettings:TimedWorkActivationSecondsCount"]);
                timer = new Timer(async o => {await PerformTimedWorkActivities(null); }, null, TimeSpan.Zero, 
                                  TimeSpan.FromSeconds(int.Parse(configuration["AppSettings:TimedWorkActivationSecondsCount"])));

            }
            catch (Exception ex1)
            {
                logger.LogError("Error occurred in BloxGuardianMessageProcessingService.Worker::StartAsync() - {0} \n {1}", ex1.Message, ex1.StackTrace);
            }       // handler 1

            await Task.Delay(0);

        }       // StartAsync();

        public override async Task StopAsync(CancellationToken cancellationToken)
        {

            logger.LogInformation("Stopped BloxChannel Message Processing Service (Assigned Processing Server ID = {0})", this.processingServerId);

            timer.Change(Timeout.Infinite, 0);

            await Task.Delay(0);

        }       // StopAsync()


        protected override async Task ExecuteAsync(CancellationToken cancellationToken)
        {

            /*

            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {

                    await streamWriter.WriteLineAsync(string.Format("Executed function at {0}", System.DateTime.Now.ToString()));
                    await streamWriter.FlushAsync();

                }
                catch (Exception ex)
                {
                    _logger.LogError("Exception occurred {0}.", ex.Message);
                }
                finally
                {
                    await Task.Delay(1000 * 5, cancellationToken);
                }
            }

            */

            // TEMP CODE
            await Task.Delay(0);

        }       // ExecuteAsync()

        private async Task PerformTimedWorkActivities(object state)
        {
 
            // NOP

        }       // PerformTimedWorkActivities()

        public async Task FromBloxChannelMessageReceivedAsync(object sender, BasicDeliverEventArgs @event)
        {

            System.Text.StringBuilder sqlStatement;


            NpgsqlConnection sqlConnection1;
            NpgsqlConnection sqlConnection2;
            //NpgsqlCommand sqlCommandGetMessageToExternalEndpoint;
            //NpgsqlDataReader sqlDataReaderGetMessageToExternalEndpoint;

            // TEMP CODE
            logger.LogDebug("Entering Worker:FromBloxChannelMessageReceivedAsync()");
            // END TEMP CODE

            try
            {

                using (sqlConnection1 = new NpgsqlConnection(configuration["ConnectionStrings:BloxChannel"]))
                {
                    await sqlConnection1.OpenAsync();

                    using (sqlConnection2 = new NpgsqlConnection(configuration["ConnectionStrings:BloxChannel"]))
                    {
                        await sqlConnection2.OpenAsync();

                        //sqlStatement = new System.Text.StringBuilder();
                        //sqlStatement.Append("SELECT CT.communication_thread_internal_id, CT.bloxchannel_communication_thread_id, CT.push_message_to_endpoint, ");
                        //sqlStatement.Append("       IGE.ingame_endpoint_bloxchannel_id, ");
                        //sqlStatement.Append("       EE.external_endpoint_internal_id, EE.external_endpoint_bloxchannel_id, EE.create_communication_thread_url, ");
                        //sqlStatement.Append("       EE.post_subsequent_message_url, EE.close_communication_thread_url, EE.parameter_style_code, ");
                        //sqlStatement.Append("       EE.endpoint_basic_auth_user_id, EE.endpoint_basic_auth_password, ");
                        //sqlStatement.Append("       MSG.message_origination_type_code, MSG.payload_type_id, MSG.payload, MSG.message_to_external_endpoint_external_id ");
                        //sqlStatement.Append("  FROM message_to_external_endpoint MSG ");
                        //sqlStatement.Append("       INNER JOIN communication_thread CT ON MSG.communication_thread_internal_id = CT.communication_thread_internal_id ");
                        //sqlStatement.Append("       INNER JOIN ingame_endpoint IGE ON CT.ingame_endpoint_internal_id = IGE.ingame_endpoint_internal_id ");
                        //sqlStatement.Append("       INNER JOIN external_endpoint EE ON CT.external_endpoint_internal_id = EE.external_endpoint_internal_id ");
                        //sqlStatement.Append("  WHERE MSG.message_to_external_endpoint_internal_id = @MessageToExternalEndpointInternalID ");

                        //sqlCommandGetMessageToExternalEndpoint = sqlConnection1.CreateCommand();
                        //sqlCommandGetMessageToExternalEndpoint.CommandText = sqlStatement.ToString();
                        //sqlCommandGetMessageToExternalEndpoint.CommandTimeout = 600;
                        //sqlCommandGetMessageToExternalEndpoint.Parameters.Add(new NpgsqlParameter("@MessageToExternalEndpointInternalID", NpgsqlTypes.NpgsqlDbType.Integer));

                        //sqlCommandGetMessageToExternalEndpoint.Parameters["@MessageToExternalEndpointInternalID"].Value = 0;
                        //await sqlCommandGetMessageToExternalEndpoint.PrepareAsync();



                        await sqlConnection2.CloseAsync();
                    }       // using (sqlConnection2 = new SqlConnection(configuration["ConnectionStrings:BloxChannel"].ToString()))

                    await sqlConnection1.CloseAsync();
                }       // using (sqlConnection2 = new NpgsqlConnection(configuration["ConnectionStrings:BloxChannel"]))

            }
            catch (Exception e)
            {

                logger.LogError("Unhandled exception occured in Worker::FromBloxChannelMessageReceivedAsync().  Message is {0}", e.Message);
                logger.LogError(e.StackTrace);

            }
            finally
            {

                logger.LogInformation("Acknowledging message queue message with delivery tag {0}", @event.DeliveryTag);
                rmqFromBloxChannelChannel.BasicAck(@event.DeliveryTag, false);

            }

        }       // FromBloxChannelMessageReceivedAsync()


        private string PadZeroLeft(int inValue, int stringLength)
        {

            string returnValue = new string('0', stringLength) + inValue.ToString();

            returnValue = returnValue.Substring(returnValue.Length - stringLength);

            return returnValue;
        }

    }
}
