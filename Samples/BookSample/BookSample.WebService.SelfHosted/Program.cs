using Microsoft.Practices.Unity;
using SyncFoundation.Core.Interfaces;
using SyncFoundation.Server;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web.Http.SelfHost;
using System.Web.Http;

namespace BookSample.WebService.SelfHosted
{
    class Program
    {
        static void Main(string[] args)
        {
            var unity = new UnityContainer();
            unity.RegisterType<SyncController>();
            unity.RegisterType<IUserService, SingleUserService>(new ContainerControlledLifetimeManager());
            unity.RegisterType<ISyncSessionDbConnectionProvider, ServerSyncSessionDbConnectionProdivder>(new ContainerControlledLifetimeManager());

            var config = new HttpSelfHostConfiguration("http://localhost:18080");
            config.DependencyResolver = new IoCContainer(unity);
            config.Routes.MapHttpRoute(
                name: "SyncRoute",
                routeTemplate: "{action}",
                defaults: new
                {
                    controller = "Sync",
                    action = "about"
                }
            );

            config.MessageHandlers.Add(new CompressedRequestHandler());
            config.MessageHandlers.Add(new CompressedResponseHandler());

            using (HttpSelfHostServer server = new HttpSelfHostServer(config))
            {
                server.OpenAsync().Wait();
                Console.WriteLine("Press Enter to quit.");
                Console.ReadLine();
            }
        }
    }
}
