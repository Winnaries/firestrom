use async_trait::async_trait;
use maelstrom::protocol::Message;
use maelstrom::{done, Node, Result, Runtime};
use message::{BroadcastCReq, FetchReqBody, FetchRespBody, FetchRespEntry, TopologyCReq};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

mod message;

pub(crate) fn main() -> Result<()> {
    Runtime::init(try_main())
}

async fn try_main() -> Result<()> {
    let handler = Arc::new(Handler::default());
    let scheduler = handler.clone();
    let runtime = Runtime::new().with_handler(handler);

    let runtime_clone = runtime.clone();

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(333));
        loop {
            interval.tick().await;
            let neighbors = scheduler.neighbors.read().unwrap().clone();
            let offsets = scheduler
                .messages
                .read()
                .unwrap()
                .iter()
                .map(|(k, v)| (k.clone(), v.len()))
                .filter(|(k, _)| k != runtime_clone.node_id())
                .collect::<HashMap<String, usize>>();
            
            let mut fetch_req = FetchReqBody::default();
            fetch_req.offsets = offsets;

            for node in neighbors {
                runtime_clone.send_async(node, fetch_req.clone()).unwrap();
            }
        }
    });

    runtime.run().await
}

#[derive(Default)]
struct Handler {
    messages: RwLock<HashMap<String, Vec<i64>>>,
    nodes: RwLock<Vec<String>>,
    neighbors: RwLock<Vec<String>>,
}

#[async_trait]
impl Node for Handler {
    async fn process(&self, runtime: Runtime, req: Message) -> Result<()> {
        if runtime.is_client(&req.src) {
            use message::ClientReq as CReq;
            match req.get_type().try_into() {
                Ok(CReq::Echo) => {
                    let resp = req.body.clone().with_type("echo_ok");
                    return runtime.reply(req, resp).await;
                }
                Ok(CReq::Generate) => {
                    let mut resp = req.body.clone().with_type("generate_ok");
                    resp.extra
                        .insert("id".into(), Uuid::new_v4().to_string().into());
                    return runtime.reply(req, resp).await;
                }
                Ok(CReq::Read) => {
                    let mut resp = req.body.clone().with_type("read_ok");
                    let messages = self.messages.read().unwrap().clone();
                    let messages = messages
                        .values()
                        .flat_map(|x| x.iter())
                        .map(|x| *x)
                        .collect::<Vec<i64>>();
                    resp.extra.insert("messages".into(), messages.into());
                    return runtime.reply(req, resp).await;
                }
                Ok(CReq::Broadcast) => {
                    let parsed = req.body.as_obj::<BroadcastCReq>().unwrap();
                    self.messages
                        .write()
                        .unwrap()
                        .get_mut(runtime.node_id())
                        .unwrap()
                        .push(parsed.message);
                    let mut resp = req.body.clone().with_type("broadcast_ok");
                    resp.extra.remove("message").unwrap();
                    return runtime.reply(req, resp).await;
                }
                Ok(CReq::Topology) => {
                    let parsed = req.body.as_obj::<TopologyCReq>().unwrap();
                    let neighbors = parsed.topology.get(runtime.node_id()).unwrap().clone();
                    let nodes = parsed.topology.keys().map(|x| x.to_string());
                    self.neighbors.write().unwrap().extend(neighbors);
                    self.nodes.write().unwrap().extend(nodes.clone());

                    for node in nodes {
                        self.messages.write().unwrap().insert(node, vec![]);
                    }

                    let mut resp = req.body.clone().with_type("topology_ok");
                    resp.extra.remove("topology").unwrap();
                    return runtime.reply(req, resp).await;
                }
                Err(_) => {}
            }
        }

        if runtime.is_from_cluster(&req.src) {
            use message::ClusterMsg::*;
            match req.get_type().into() {
                FetchReq => {
                    let FetchReqBody { offsets, .. } = req.body.as_obj::<FetchReqBody>().unwrap();
                    let target = offsets
                        .keys()
                        .map(|x| x.to_string())
                        .collect::<Vec<String>>();
                    let mut resp = FetchRespBody::default();

                    let messages = self.messages.read().unwrap().clone();
                    let slices = messages
                        .into_iter()
                        .filter(|(k, _)| target.iter().any(|x| x == k))
                        .map(|(k, v)| {
                            (
                                k.clone(),
                                v.into_iter()
                                    .skip(*offsets.get(&k).unwrap())
                                    .collect::<Vec<i64>>(),
                            )
                        })
                        .filter(|(_, v)| !v.is_empty())
                        .map(|(k, v)| {
                            (
                                k.clone(),
                                FetchRespEntry {
                                    offset: *offsets.get(&k).unwrap(),
                                    messages: v,
                                },
                            )
                        })
                        .collect::<HashMap<String, FetchRespEntry>>();

                    resp.messages = slices;
                    return runtime.reply(req, resp).await;
                }
                FetchResp => {
                    let FetchRespBody {
                        messages: received, ..
                    } = req.body.as_obj::<FetchRespBody>().unwrap();
                    let mut messages = self.messages.write().unwrap();

                    for (k, v) in received {
                        let entry = messages.get_mut(&k).unwrap();
                        let start = v.offset;
                        let end = start + v.messages.len();

                        if end > entry.len() {
                            let nyr = end - entry.len();
                            entry.extend_from_slice(&v.messages[v.messages.len() - nyr..]);
                        }
                    }

                    return Ok(());
                }
            }
        }

        done(runtime, req)
    }
}
