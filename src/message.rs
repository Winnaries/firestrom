use serde::{Serialize, Deserialize};
use std::collections::HashMap;

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub(crate) enum ClientReq {
    Echo,  
    Generate, 
    Broadcast, 
    Read, 
    Topology, 
}

impl TryFrom<&str> for ClientReq {
    type Error = String; 

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "echo" => Ok(ClientReq::Echo), 
            "generate" => Ok(ClientReq::Generate), 
            "broadcast" => Ok(ClientReq::Broadcast), 
            "read" => Ok(ClientReq::Read), 
            "topology" => Ok(ClientReq::Topology), 
            _ => Err(format!("Unsupported client request: {}", s)), 
        }
    }
}

impl From<ClientReq> for &str {
    fn from(req: ClientReq) -> Self {
        match req {
            ClientReq::Echo => "echo", 
            ClientReq::Generate => "generate", 
            ClientReq::Broadcast => "broadcast", 
            ClientReq::Read => "read", 
            ClientReq::Topology => "topology", 
        }
    }
}

impl From<ClientReq> for String {
    fn from(req: ClientReq) -> Self {
        match req {
            ClientReq::Echo => "echo".into(), 
            ClientReq::Generate => "generate".into(), 
            ClientReq::Broadcast => "broadcast".into(), 
            ClientReq::Read => "read".into(), 
            ClientReq::Topology => "topology".into(), 
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub struct BroadcastCReq {
    pub message: i64, 
}

#[derive(Serialize, Deserialize)]
pub struct TopologyCReq {
    pub topology: HashMap<String, Vec<String>>, 
}

#[derive(Copy, Clone)]
pub enum ClusterMsg {
    FetchReq, 
    FetchResp, 
}

impl From<&str> for ClusterMsg {
    fn from(s: &str) -> Self {
        match s {
            "fetch_req" => ClusterMsg::FetchReq, 
            "fetch_resp" => ClusterMsg::FetchResp, 
            _ => panic!("Invalid cluster message"), 
        }
    }
}

impl From<ClusterMsg> for &str {
    fn from(msg: ClusterMsg) -> Self {
        match msg {
            ClusterMsg::FetchReq => "fetch_req", 
            ClusterMsg::FetchResp => "fetch_resp", 
        }
    }
}

impl From<ClusterMsg> for String {
    fn from(msg: ClusterMsg) -> Self {
        match msg {
            ClusterMsg::FetchReq => "fetch_req".into(), 
            ClusterMsg::FetchResp => "fetch_resp".into(), 
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct FetchReqBody {
    #[serde(rename = "type")]
    pub typ: String, 
    #[serde(flatten)]
    pub offsets: HashMap<String, usize>, 
}

impl Default for FetchReqBody {
    fn default() -> Self {
        FetchReqBody {
            typ: "fetch_req".into(), 
            offsets: HashMap::new(), 
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct FetchRespBody {
    #[serde(rename = "type")]
    pub typ: String, 
    #[serde(flatten)]
    pub messages: HashMap<String, FetchRespEntry>, 
}

impl Default for FetchRespBody {
    fn default() -> Self {
        FetchRespBody {
            typ: "fetch_resp".into(), 
            messages: HashMap::new(), 
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct FetchRespEntry {
    pub offset: usize, 
    pub messages: Vec<i64>,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_converting_str_to_creq() {
        assert_eq!(ClientReq::Echo, "echo".try_into().unwrap());
        assert_eq!(ClientReq::Generate, "generate".try_into().unwrap());
        assert_eq!(ClientReq::Broadcast, "broadcast".try_into().unwrap());
        assert_eq!(ClientReq::Read, "read".try_into().unwrap());
        assert_eq!(ClientReq::Topology, "topology".try_into().unwrap());
    }

}