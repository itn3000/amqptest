using System;
using Amqp;
using Amqp.Framing;
using System.Threading;
using System.Threading.Tasks;
using Amqp.Listener;
using System.Linq;

namespace ConsoleApplication
{
    public class Program
    {
        class MyEndPoint : LinkEndpoint
        {
            public override void OnDisposition(DispositionContext dispositionContext)
            {
            }

            public override void OnFlow(FlowContext flowContext)
            {
            }

            public override async void OnMessage(MessageContext messageContext)
            {
                messageContext.Complete();
                await Task.Delay(1000);
                System.Diagnostics.Debug.WriteLine($"After OnMessage:{messageContext.Message.Body.ToString()}");
            }
        }
        class MyOutEndPoint : LinkEndpoint
        {
            public override void OnDisposition(DispositionContext dispositionContext)
            {
                dispositionContext.Complete();
            }

            public override void OnFlow(FlowContext flowContext)
            {
                for (int i = 0; i < flowContext.Messages; i++)
                {
                    var msg = new Message("outgoing message");
                    msg.Properties = new Properties() { Subject = "Message0" };
                    flowContext.Link.SendMessage(msg);
                }
            }
        }
        class MyLinkProcessor : ILinkProcessor
        {
            public async void Process(AttachContext attachContext)
            {
                await ProcessAsync(attachContext);
            }
            Task ProcessAsync(AttachContext ctx)
            {
                return Task.Run(() =>
                {
                    System.Diagnostics.Debug.WriteLine("in processasync");
                    if (ctx.Attach.LinkName == "")
                    {
                        System.Diagnostics.Debug.WriteLine("a");
                        ctx.Complete(new Error() { Condition = ErrorCode.InvalidField, Description = "Empty link name not allowed." });
                    }
                    else if (ctx.Link.Role)
                    {
                        System.Diagnostics.Debug.WriteLine($"b:{ctx.Attach.LinkName}");
                        // receiver
                        var tgt = ctx.Attach.Target as Target;
                        if (tgt != null)
                        {
                            System.Diagnostics.Debug.WriteLine($"tgt={tgt.Address}");
                            ctx.Complete(new MyEndPoint(), 5);
                        }
                        else
                        {
                            ctx.Complete(new Error { Condition = ErrorCode.InvalidField, Description = "target is null" });
                        }
                    }
                    else
                    {
                        System.Diagnostics.Debug.WriteLine("c");
                        ctx.Complete(new MyOutEndPoint(), 0);
                    }
                });
            }
        }
        static async Task OpenContainerHost(string address, CancellationToken ctoken, ManualResetEventSlim ev)
        {
            var host = new ContainerHost(new Uri(address));
            try
            {
                host.Open();
                host.RegisterLinkProcessor(new MyLinkProcessor());
                ev.Set();
                System.Diagnostics.Debug.WriteLine($"register end on {address}");
                await Task.Delay(-1, ctoken);
                System.Diagnostics.Debug.WriteLine("wait end");
            }
            catch (Exception e)
            {
                System.Diagnostics.Debug.WriteLine($"{e.ToString()}");
            }
            finally
            {
                host.Close();
            }
        }
        public static void Main(string[] args)
        {
            using (var csrc = new CancellationTokenSource())
            using (var ev = new ManualResetEventSlim(false))
            {
                var addr = "amqp://guest:guest@127.0.0.1:9001";
                var srvTask = OpenContainerHost(addr, csrc.Token, ev);
                // wait for ready
                ev.Wait();
                var allsw = new System.Diagnostics.Stopwatch();
                allsw.Start();
                Task.WhenAll(Enumerable.Range(0, 300).Select(async i =>
                 {
                     var sw = new System.Diagnostics.Stopwatch();
                     sw.Start();
                     var client = new Connection(new Address(addr));
                     try
                     {
                         var session = new Session(client);
                         var senderLink = new SenderLink(session, "MyLinkName", "my-queue");
                         var msg = new Message($"Hello world{i}");
                         await senderLink.SendAsync(msg);
                         System.Diagnostics.Debug.WriteLine($"send end{i}");
                     }
                     finally
                     {
                         await client.CloseAsync();
                         sw.Stop();
                         System.Diagnostics.Debug.WriteLine($"client{i} elapsed:{sw.Elapsed}");
                     }
                 }).ToArray()).Wait();
                allsw.Stop();
                Console.WriteLine($"all client end,elapsed={allsw.Elapsed}");
                csrc.Cancel();
                srvTask.Wait();
            }
            Console.WriteLine("end execute");
        }
    }
}
