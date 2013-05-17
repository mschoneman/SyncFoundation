using System.Web.Http;
using Microsoft.Practices.Unity;
using SyncFoundation.Server;
using SyncFoundation.Core.Interfaces;

namespace BookSample.WebService
{
    public static class Bootstrapper
    {
        public static void Initialise()
        {
            var container = BuildUnityContainer();

            GlobalConfiguration.Configuration.DependencyResolver = new Unity.WebApi.UnityDependencyResolver(container);
        }

        private static IUnityContainer BuildUnityContainer()
        {
            var container = new UnityContainer();

            container.RegisterType<IUserService, SingleUserService>(new ContainerControlledLifetimeManager());
            container.RegisterType<ISyncSessionDbConnectionProvider, ServerSyncSessionDbConnectionProdivder>(new ContainerControlledLifetimeManager());

            return container;
        }
    }
}