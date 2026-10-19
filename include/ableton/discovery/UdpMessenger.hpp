/* Copyright 2016, Ableton AG, Berlin. All rights reserved.
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *  If you would like to incorporate Link into a proprietary software application,
 *  please contact <link-devs@ableton.com>.
 */

#pragma once

#include <ableton/discovery/AsioTypes.hpp>
#include <ableton/discovery/IpInterface.hpp>
#include <ableton/discovery/MessageTypes.hpp>
#include <ableton/discovery/v1/Messages.hpp>
#include <ableton/util/Injected.hpp>
#include <ableton/util/SafeAsyncHandler.hpp>
#include <algorithm>
#include <chrono>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

namespace ableton
{
namespace discovery
{

// An exception thrown when sending a udp message fails. Stores the
// interface through which the sending failed.
struct UdpSendException : std::runtime_error
{
  UdpSendException(const std::runtime_error& e, IpAddress ifAddr)
    : std::runtime_error(e.what())
    , interfaceAddr(std::move(ifAddr))
  {
  }

  IpAddress interfaceAddr;
};

template <typename Interface>
UdpEndpoint ipV6Endpoint(Interface& iface, const UdpEndpoint& endpoint)
{
  auto v6Address = endpoint.address().to_v6();
  v6Address.scope_id(iface.endpoint().address().to_v6().scope_id());
  return {v6Address, endpoint.port()};
}

// Throws UdpSendException
template <typename Interface, typename NodeId, typename Payload>
void sendUdpMessage(Interface& iface,
                    NodeId from,
                    const uint8_t ttl,
                    const v1::MessageType messageType,
                    const Payload& payload,
                    const UdpEndpoint& to)
{
  using namespace std;
  v1::MessageBuffer buffer;
  const auto messageBegin = begin(buffer);
  const auto messageEnd =
    v1::detail::encodeMessage(std::move(from), ttl, messageType, payload, messageBegin);
  const auto numBytes = static_cast<size_t>(distance(messageBegin, messageEnd));
  iface.send(buffer.data(), numBytes, to);
}

// UdpMessenger uses a "shared_ptr pImpl" pattern to make it movable
// and to support safe async handler callbacks when receiving messages
// on the given interface.
template <typename Interface, typename NodeStateT, typename IoContext>
class UdpMessenger
{
public:
  using NodeState = NodeStateT;
  using NodeId = typename NodeState::IdType;
  using Timer = typename util::Injected<IoContext>::type::Timer;
  using TimerError = typename Timer::ErrorCode;
  using TimePoint = typename Timer::TimePoint;

  UdpMessenger(util::Injected<Interface> iface,
               NodeState state,
               util::Injected<IoContext> io,
               const uint8_t ttl,
               const uint8_t ttlRatio)
    : mpImpl(std::make_shared<Impl>(
        std::move(iface), std::move(state), std::move(io), ttl, ttlRatio))
  {
    // We need to always listen for incoming traffic in order to
    // respond to peer state broadcasts
    mpImpl->listen(MulticastTag{});
    mpImpl->listen(UnicastTag{});
    mpImpl->broadcastState();
  }

  UdpMessenger(const UdpMessenger&) = delete;
  UdpMessenger& operator=(const UdpMessenger&) = delete;

  UdpMessenger(UdpMessenger&& rhs)
    : mpImpl(std::move(rhs.mpImpl))
  {
  }

  ~UdpMessenger()
  {
    if (mpImpl != nullptr)
    {
      mpImpl->sendByeBye();
    }
  }

  void updateState(NodeState state) { mpImpl->updateState(std::move(state)); }

  // Broadcast the current state of the system to all peers. May throw
  // std::runtime_error if assembling a broadcast message fails or if
  // there is an error at the transport layer. Throws on failure.
  void broadcastState() { mpImpl->broadcastState(); }

  // Asynchronous receive function for incoming messages from peers. Will
  // return immediately and the handler will be invoked when a message
  // is received. Handler must have operator() overloads for PeerState and
  // ByeBye messages.
  template <typename Handler>
  void receive(Handler handler)
  {
    mpImpl->setReceiveHandler(std::move(handler));
  }

private:
  struct Impl : std::enable_shared_from_this<Impl>
  {
    Impl(util::Injected<Interface> iface,
         NodeState state,
         util::Injected<IoContext> io,
         const uint8_t ttl,
         const uint8_t ttlRatio)
      : mIo(std::move(io))
      , mInterface(std::move(iface))
      , mState(std::move(state))
      , mTimer(mIo->makeTimer())
      , mCoalesceTimer(mIo->makeTimer())
      , mCoalesceArmed(false)
      , mLastBroadcastTime{}
      , mTtl(ttl)
      , mTtlRatio(ttlRatio)
      , mPeerStateHandler([](PeerState<NodeState>) {})
      , mByeByeHandler([](ByeBye<NodeId>) {})
    {
    }

    template <typename Handler>
    void setReceiveHandler(Handler handler)
    {
      mPeerStateHandler = [handler](PeerState<NodeState> state)
      { handler(std::move(state)); };

      mByeByeHandler = [handler](ByeBye<NodeId> byeBye) { handler(std::move(byeBye)); };
    }

    void sendByeBye()
    {
      if (mInterface->endpoint().address().is_v4())
      {
        sendUdpMessage(*mInterface,
                       mState.ident(),
                       0,
                       v1::kByeBye,
                       makePayload(),
                       multicastEndpointV4());
      }
      if (mInterface->endpoint().address().is_v6())
      {
        sendUdpMessage(
          *mInterface,
          mState.ident(),
          0,
          v1::kByeBye,
          makePayload(),
          multicastEndpointV6(mInterface->endpoint().address().to_v6().scope_id()));
      }
    }

    void updateState(NodeState state) { mState = std::move(state); }

    void broadcastState()
    {
      using namespace std::chrono;

      // tbd: 100 ms (upstream 50 ms). During a tempo drag on a peer every adopted change
      // rebroadcasts our state; each send is network code from flash on a core that shares
      // its cache with the audio render.
      const auto minBroadcastPeriod = milliseconds{100};
      const auto nominalBroadcastPeriod = milliseconds(mTtl * 1000 / mTtlRatio);
      const auto timeSinceLastBroadcast =
        duration_cast<milliseconds>(mTimer.now() - mLastBroadcastTime);

      // The rate is limited to maxBroadcastRate to prevent flooding the network.
      const auto delay = minBroadcastPeriod - timeSinceLastBroadcast;

      // Schedule the next broadcast before we actually send the
      // message so that if sending throws an exception we are still
      // scheduled to try again. We want to keep trying at our
      // interval as long as this instance is alive.
      mTimer.expires_from_now(delay > milliseconds{0} ? delay : nominalBroadcastPeriod);
      mTimer.async_wait(
        [this](const TimerError e)
        {
          if (!e)
          {
            broadcastState();
          }
        });

      // If we're not delaying, broadcast now
      if (delay < milliseconds{1})
      {
        debug(mIo->log()) << "Broadcasting state";
        if (mInterface->endpoint().address().is_v4())
        {
          sendPeerState(v1::kAlive, multicastEndpointV4());
        }
        if (mInterface->endpoint().address().is_v6())
        {
          sendPeerState(
            v1::kAlive,
            multicastEndpointV6(mInterface->endpoint().address().to_v6().scope_id()));
        }
      }
    }

    void sendPeerState(const v1::MessageType messageType, const UdpEndpoint& to)
    {
      sendUdpMessage(
        *mInterface, mState.ident(), mTtl, messageType, toPayload(mState), to);
      mLastBroadcastTime = mTimer.now();
    }

    void sendResponse(const UdpEndpoint& to)
    {
      const auto endpoint = to.address().is_v4() ? to : ipV6Endpoint(*mInterface, to);
      sendPeerState(v1::kResponse, endpoint);
    }

    template <typename Tag>
    void listen(Tag tag)
    {
      mInterface->receive(util::makeAsyncSafe(this->shared_from_this()), tag);
    }

    template <typename Tag, typename It>
    void operator()(Tag tag,
                    const UdpEndpoint& from,
                    const It messageBegin,
                    const It messageEnd)
    {
      auto result = v1::parseMessageHeader<NodeId>(messageBegin, messageEnd);

      const auto& header = result.first;
      // Ignore messages from self and other groups
      if (header.ident != mState.ident() && header.groupId == 0)
      {
        // On Linux multicast messages are sent to all sockets registered to the multicast
        // group. To avoid duplicate message handling and invalid response messages we
        // check if the message is coming from an endpoint that is in the same subnet as
        // the interface.
        auto ignoreIpV4Message = false;
        if (from.address().is_v4() && mInterface->endpoint().address().is_v4())
        {
          const auto subnet = LINK_ASIO_NAMESPACE::ip::make_network_v4(
            mInterface->endpoint().address().to_v4(), 24);
          const auto fromAddr =
            LINK_ASIO_NAMESPACE::ip::make_network_v4(from.address().to_v4(), 32);
          ignoreIpV4Message = !fromAddr.is_subnet_of(subnet);
        }

        if (!ignoreIpV4Message)
        {
          debug(mIo->log()) << "Received message type "
                            << static_cast<int>(header.messageType) << " from peer "
                            << header.ident;

          switch (header.messageType)
          {
          case v1::kAlive:
            // tbd: a response tells a newcomer about us before our next alive; once a
            // second per peer is enough, not one per alive (a tempo drag sends up to 20).
            if (shouldRespond(header.ident))
            {
              sendResponse(from);
            }
            receivePeerState(std::move(result.first), result.second, messageEnd);
            break;
          case v1::kResponse:
            receivePeerState(std::move(result.first), result.second, messageEnd);
            break;
          case v1::kByeBye:
            forgetPeer(result.first.ident);
            receiveByeBye(std::move(result.first.ident));
            break;
          default:
            info(mIo->log()) << "Unknown message received of type: "
                             << header.messageType;
          }
        }
      }
      listen(tag);
    }

    template <typename It>
    void receivePeerState(v1::MessageHeader<NodeId> header,
                          It payloadBegin,
                          It payloadEnd)
    {

      bool wellFormed = false;
      auto state = NodeState::fromPayload(std::move(header.ident),
        std::move(payloadBegin), std::move(payloadEnd), &wellFormed);

      // A malformed payload leaves default entries behind -- a zero tempo and
      // an empty session id would enter the session as a peer state. Upstream
      // drops the message by letting the parse throw past this point.
      if (!wellFormed)
      {
        return;
      }

      offerPeerState(PeerState<NodeState>{std::move(state), header.ttl});
    }

    // tbd: coalescing. A peer's state reaches the gateway at most once per
    // coalescePeriod(); a state arriving sooner is held, and the newest held one is
    // delivered when the period ends. Link's peer state is a whole state (the last one
    // wins), so nothing is lost, only merged; a bye-bye drops what is held for that peer.
    static std::chrono::milliseconds coalescePeriod() { return std::chrono::milliseconds{100}; }
    static std::chrono::milliseconds responseInterval() { return std::chrono::milliseconds{1000}; }

    using PeerId =
      typename std::decay<decltype(std::declval<const NodeState&>().ident())>::type;

    struct PeerEntry
    {
      PeerId id;
      TimePoint lastDelivered;
      TimePoint lastResponded;
      bool delivered;
      bool responded;
      bool pending;
      PeerState<NodeState> pendingState;
    };

    PeerEntry& peerEntry(const PeerId& id, const TimePoint now)
    {
      for (auto& entry : mPeers)
      {
        if (entry.id == id)
        {
          return entry;
        }
      }
      // Forget peers we have not heard from in a minute, so the list stays small.
      mPeers.erase(std::remove_if(mPeers.begin(),
                                  mPeers.end(),
                                  [now](const PeerEntry& e)
                                  {
                                    return !e.pending
                                           && now - e.lastDelivered > std::chrono::seconds(60)
                                           && now - e.lastResponded > std::chrono::seconds(60);
                                  }),
                   mPeers.end());
      mPeers.push_back(PeerEntry{id, now, now, false, false, false, {}});
      return mPeers.back();
    }

    bool shouldRespond(const PeerId& id)
    {
      const auto now = mTimer.now();
      auto& entry = peerEntry(id, now);
      if (entry.responded && now - entry.lastResponded < responseInterval())
      {
        return false;
      }
      entry.responded = true;
      entry.lastResponded = now;
      return true;
    }

    void forgetPeer(const PeerId& id)
    {
      mPeers.erase(std::remove_if(mPeers.begin(),
                                  mPeers.end(),
                                  [&id](const PeerEntry& e) { return e.id == id; }),
                   mPeers.end());
    }

    void deliverPeerState(PeerState<NodeState> peerState)
    {
      // Handlers must only be called once
      auto handler = std::move(mPeerStateHandler);
      mPeerStateHandler = [](PeerState<NodeState>) {};
      handler(std::move(peerState));
    }

    void offerPeerState(PeerState<NodeState> peerState)
    {
      const auto now = mTimer.now();
      auto& entry = peerEntry(peerState.peerState.ident(), now);
      if (!entry.pending && (!entry.delivered || now - entry.lastDelivered >= coalescePeriod()))
      {
        entry.delivered = true;
        entry.lastDelivered = now;
        deliverPeerState(std::move(peerState));
        return;
      }
      entry.pendingState = std::move(peerState);
      entry.pending = true;
      scheduleCoalesced(now);
    }

    void scheduleCoalesced(const TimePoint now)
    {
      if (mCoalesceArmed)
      {
        return;
      }
      auto wait = coalescePeriod();
      for (const auto& entry : mPeers)
      {
        if (entry.pending)
        {
          const auto due = std::chrono::duration_cast<std::chrono::milliseconds>(
            entry.lastDelivered + coalescePeriod() - now);
          if (due < wait)
          {
            wait = due;
          }
        }
      }
      if (wait < std::chrono::milliseconds{1})
      {
        wait = std::chrono::milliseconds{1};
      }
      mCoalesceArmed = true;
      mCoalesceTimer.expires_from_now(wait);
      mCoalesceTimer.async_wait(
        [this](const TimerError e)
        {
          mCoalesceArmed = false;
          if (!e)
          {
            deliverDue();
          }
        });
    }

    void deliverDue()
    {
      const auto now = mTimer.now();
      // Deliver from a copy of the due states: a delivery may not touch mPeers, but the
      // list must not be walked while a handler runs.
      std::vector<PeerState<NodeState>> due;
      for (auto& entry : mPeers)
      {
        if (entry.pending && now - entry.lastDelivered >= coalescePeriod())
        {
          entry.pending = false;
          entry.delivered = true;
          entry.lastDelivered = now;
          due.push_back(std::move(entry.pendingState));
        }
      }
      for (auto& peerState : due)
      {
        deliverPeerState(std::move(peerState));
      }
      if (std::any_of(mPeers.begin(), mPeers.end(), [](const PeerEntry& e) { return e.pending; }))
      {
        scheduleCoalesced(now);
      }
    }

    void receiveByeBye(NodeId nodeId)
    {
      // Handlers must only be called once
      auto byeByeHandler = std::move(mByeByeHandler);
      mByeByeHandler = [](ByeBye<NodeId>) {};
      byeByeHandler(ByeBye<NodeId>{std::move(nodeId)});
    }

    util::Injected<IoContext> mIo;
    util::Injected<Interface> mInterface;
    NodeState mState;
    Timer mTimer;
    Timer mCoalesceTimer;
    bool mCoalesceArmed;
    std::vector<PeerEntry> mPeers;
    TimePoint mLastBroadcastTime;
    uint8_t mTtl;
    uint8_t mTtlRatio;
    std::function<void(PeerState<NodeState>)> mPeerStateHandler;
    std::function<void(ByeBye<NodeId>)> mByeByeHandler;
  };

  std::shared_ptr<Impl> mpImpl;
};

// Factory function
template <typename Interface, typename NodeState, typename IoContext>
UdpMessenger<Interface, NodeState, IoContext> makeUdpMessenger(
  util::Injected<Interface> iface,
  NodeState state,
  util::Injected<IoContext> io,
  const uint8_t ttl,
  const uint8_t ttlRatio)
{
  return UdpMessenger<Interface, NodeState, IoContext>{
    std::move(iface), std::move(state), std::move(io), ttl, ttlRatio};
}

} // namespace discovery
} // namespace ableton
