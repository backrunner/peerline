use super::{
    Candidate, DiscoveryConfig,
    endpoints::{
        LocalDirectNetworks, descriptor_candidates_for_discovery,
        discovered_direct_endpoint_candidates,
    },
    now_unix_ms, rank_candidates,
};
use libp2p::PeerId;
use peerline_rendezvous_model::PeerDescriptor;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
};

pub(super) struct DiscoverySnapshot {
    pub(super) observed_peers: HashSet<PeerId>,
    pub(super) local_peer_ids: HashSet<String>,
    pub(super) descriptors: HashMap<String, PeerDescriptor>,
}

impl DiscoverySnapshot {
    pub(super) fn new() -> Self {
        Self {
            observed_peers: HashSet::new(),
            local_peer_ids: HashSet::new(),
            descriptors: HashMap::new(),
        }
    }

    pub(super) fn is_diverse_enough(&self, minimum: usize) -> bool {
        if self.descriptors.is_empty() {
            return false;
        }
        self.has_local_descriptor() || self.observed_peers.len() >= minimum
    }

    pub(super) fn observe_local_peer(&mut self, peer: PeerId) {
        self.local_peer_ids.insert(peer.to_string());
        self.observed_peers.insert(peer);
    }

    fn has_local_descriptor(&self) -> bool {
        self.descriptors
            .keys()
            .any(|peer_id| self.local_peer_ids.contains(peer_id))
    }

    pub(super) fn insert_descriptor(&mut self, descriptor: PeerDescriptor) {
        let Ok(peer_id) = descriptor.peer_id.parse::<PeerId>() else {
            return;
        };
        let mut descriptor = descriptor;
        descriptor.published_unix_ms = descriptor.published_unix_ms.min(now_unix_ms());
        match self.descriptors.get(&descriptor.peer_id) {
            Some(current) if current.published_unix_ms > descriptor.published_unix_ms => {}
            _ => {
                self.observed_peers.insert(peer_id);
                self.descriptors
                    .insert(descriptor.peer_id.clone(), descriptor);
            }
        }
    }

    pub(super) fn best_descriptor(
        &self,
        local_networks: Option<&LocalDirectNetworks>,
        config: &DiscoveryConfig,
    ) -> Option<PeerDescriptor> {
        self.descriptors
            .values()
            .filter(|descriptor| {
                let allow_unverified_lan = self.local_peer_ids.contains(&descriptor.peer_id);
                !descriptor_candidates_for_discovery(
                    descriptor,
                    local_networks,
                    allow_unverified_lan,
                    config,
                )
                .is_empty()
            })
            .cloned()
            .max_by_key(|descriptor| {
                let allow_unverified_lan = self.local_peer_ids.contains(&descriptor.peer_id);
                (
                    descriptor.published_unix_ms,
                    descriptor_candidates_for_discovery(
                        descriptor,
                        local_networks,
                        allow_unverified_lan,
                        config,
                    )
                    .len(),
                    descriptor.peer_id.clone(),
                )
            })
    }

    pub(super) fn best_direct_endpoints(
        &self,
        local_networks: Option<&LocalDirectNetworks>,
    ) -> Vec<SocketAddr> {
        self.descriptors
            .values()
            .cloned()
            .filter(|descriptor| {
                let allow_unverified_lan = self.local_peer_ids.contains(&descriptor.peer_id);
                !discovered_direct_endpoint_candidates(
                    descriptor,
                    local_networks,
                    allow_unverified_lan,
                )
                .is_empty()
            })
            .max_by_key(|descriptor| {
                let allow_unverified_lan = self.local_peer_ids.contains(&descriptor.peer_id);
                (
                    descriptor.published_unix_ms,
                    discovered_direct_endpoint_candidates(
                        descriptor,
                        local_networks,
                        allow_unverified_lan,
                    )
                    .len(),
                    descriptor.peer_id.clone(),
                )
            })
            .map(|descriptor| {
                let allow_unverified_lan = self.local_peer_ids.contains(&descriptor.peer_id);
                discovered_direct_endpoint_candidates(
                    &descriptor,
                    local_networks,
                    allow_unverified_lan,
                )
            })
            .unwrap_or_default()
    }

    pub(super) fn has_usable_candidates(
        &self,
        local_networks: Option<&LocalDirectNetworks>,
        config: &DiscoveryConfig,
    ) -> bool {
        self.descriptors.values().any(|descriptor| {
            let allow_unverified_lan = self.local_peer_ids.contains(&descriptor.peer_id);
            !descriptor_candidates_for_discovery(
                descriptor,
                local_networks,
                allow_unverified_lan,
                config,
            )
            .is_empty()
        })
    }

    pub(super) fn into_candidates(
        self,
        local_networks: Option<&LocalDirectNetworks>,
        config: &DiscoveryConfig,
    ) -> Vec<Candidate> {
        let local_peer_ids = self.local_peer_ids;
        rank_candidates(self.descriptors.into_values().flat_map(|descriptor| {
            let allow_unverified_lan = local_peer_ids.contains(&descriptor.peer_id);
            descriptor_candidates_for_discovery(
                &descriptor,
                local_networks,
                allow_unverified_lan,
                config,
            )
        }))
    }

    pub(super) fn candidates(
        &self,
        local_networks: Option<&LocalDirectNetworks>,
        config: &DiscoveryConfig,
    ) -> Vec<Candidate> {
        rank_candidates(self.descriptors.values().flat_map(|descriptor| {
            let allow_unverified_lan = self.local_peer_ids.contains(&descriptor.peer_id);
            descriptor_candidates_for_discovery(
                descriptor,
                local_networks,
                allow_unverified_lan,
                config,
            )
        }))
    }
}
