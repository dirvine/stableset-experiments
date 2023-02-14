mod fake_crypto;
mod handover;
mod membership;
mod stable_set;

use std::{borrow::Cow, collections::BTreeSet};

use handover::Handover;
use membership::Membership;
use stateright::{
    actor::{model_peers, Actor, ActorModel, Id, Network, Out},
    Expectation, Model,
};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct State {
    pub handover: Handover,
    pub membership: Membership,
}

#[derive(Clone)]
pub struct Node {
    pub genesis_nodes: BTreeSet<Id>,
    pub peers: Vec<Id>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Msg {
    Membership(membership::Msg),
    Handover(handover::Msg),
}

impl From<membership::Msg> for Msg {
    fn from(msg: membership::Msg) -> Self {
        Self::Membership(msg)
    }
}

impl From<handover::Msg> for Msg {
    fn from(msg: handover::Msg) -> Self {
        Self::Handover(msg)
    }
}

impl Actor for Node {
    type Msg = Msg;
    type State = State;

    fn on_start(&self, id: Id, o: &mut Out<Self>) -> Self::State {
        let membership = Membership::new(&self.genesis_nodes);
        let handover = Handover::new(self.genesis_nodes.clone());

        if !self.genesis_nodes.contains(&id) {
            o.broadcast(&self.genesis_nodes, &membership.req_join(id).into());
        }

        State {
            handover,
            membership,
        }
    }

    fn on_msg(
        &self,
        id: Id,
        state: &mut Cow<Self::State>,
        src: Id,
        msg: Self::Msg,
        o: &mut Out<Self>,
    ) {
        match msg {
            Msg::Membership(msg) => {
                let elders = state.handover.elders();
                state.to_mut().membership.on_msg(&elders, id, src, msg, o);
            }
            Msg::Handover(msg) => state.to_mut().handover.on_msg(id, src, msg, o),
        }
    }
}

#[derive(Clone)]
struct ModelCfg {
    elder_count: usize,
    server_count: usize,
    network: Network<<Node as Actor>::Msg>,
}

impl ModelCfg {
    fn into_model(self) -> ActorModel<Node, Self, Vec<Msg>> {
        ActorModel::new(self.clone(), vec![])
            .actors((0..self.server_count).map(|i| Node {
                genesis_nodes: BTreeSet::from_iter((0..self.elder_count).into_iter().map(Id::from)),
                peers: model_peers(i, self.server_count),
            }))
            .init_network(self.network)
            .property(
                Expectation::Eventually,
                "everyone joined and converged",
                |_, state| {
                    let reference_stable_set = state.actor_states[0].membership.stable_set.clone();

                    let all_nodes_joined = (0..state.actor_states.len())
                        .into_iter()
                        .map(Id::from)
                        .all(|id| reference_stable_set.contains(id));

                    if !all_nodes_joined {
                        return false;
                    }

                    for state in state.actor_states.iter() {
                        if reference_stable_set != state.membership.stable_set {
                            return false;
                        }
                    }

                    true
                },
            )
    }
}

fn main() {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    let network = Network::new_unordered_nonduplicating([]);

    ModelCfg {
        elder_count: 2,
        server_count: 4,
        network,
    }
    .into_model()
    .checker()
    .threads(num_cpus::get())
    .serve("localhost:3000");
}
