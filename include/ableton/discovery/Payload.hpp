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

#include <ableton/discovery/NetworkByteStreamSerializable.hpp>
#include <functional>
#include <unordered_map>

namespace ableton
{
namespace discovery
{

struct PayloadEntryHeader
{
  using Key = std::uint32_t;
  using Size = std::uint32_t;

  Key key;
  Size size;

  friend Size sizeInByteStream(const PayloadEntryHeader& header)
  {
    return sizeInByteStream(header.key) + sizeInByteStream(header.size);
  }

  template <typename It>
  friend It toNetworkByteStream(const PayloadEntryHeader& header, It out)
  {
    return toNetworkByteStream(
      header.size, toNetworkByteStream(header.key, std::move(out)));
  }

  template <typename It>
  static std::pair<PayloadEntryHeader, It> fromNetworkByteStream(It begin, const It end)
  {
    using namespace std;
    Key key;
    Size size;
    tie(key, begin) = Deserialize<Key>::fromNetworkByteStream(begin, end);
    tie(size, begin) = Deserialize<Size>::fromNetworkByteStream(begin, end);
    return make_pair(
      PayloadEntryHeader{std::move(key), std::move(size)}, std::move(begin));
  }
};

template <typename EntryType>
struct PayloadEntry
{
  PayloadEntry(EntryType entryVal)
    : value(std::move(entryVal))
  {
    header = {EntryType::key, sizeInByteStream(value)};
  }

  PayloadEntryHeader header;
  EntryType value;

  friend std::uint32_t sizeInByteStream(const PayloadEntry& entry)
  {
    return sizeInByteStream(entry.header) + sizeInByteStream(entry.value);
  }

  template <typename It>
  friend It toNetworkByteStream(const PayloadEntry& entry, It out)
  {
    // Don't serialize Entry if its value is of size zero
    if (sizeInByteStream(entry.value) == 0)
    {
      return out;
    }
    return toNetworkByteStream(
      entry.value, toNetworkByteStream(entry.header, std::move(out)));
  }
};

namespace detail
{

// A handler returns false to reject its entry, which rejects the payload;
// upstream signals the same condition by throwing out of the handler.
template <typename It>
using HandlerMap =
  std::unordered_map<typename PayloadEntryHeader::Key, std::function<bool(It, It)>>;

// Given an index of handlers and a byte range, parse the bytes as a
// sequence of payload entries and invoke the appropriate handler for
// each entry type. Entries that are encountered that do not have a
// corresponding handler in the map are ignored. Returns false if the
// bytes are malformed, where upstream throws std::runtime_error. Note
// that some of the handlers may already have been called.
template <typename It>
bool parseByteStream(HandlerMap<It>& map, It bsBegin, const It bsEnd)
{
  using namespace std;
  using ItDiff = typename iterator_traits<It>::difference_type;

  // An entry header is a key and a size, each a uint32 on the wire.
  const auto headerSize = static_cast<ItDiff>(sizeof(PayloadEntryHeader::Key)
                                              + sizeof(PayloadEntryHeader::Size));

  while (bsBegin < bsEnd)
  {
    // A truncated entry header ends the payload here.
    if (distance(bsBegin, bsEnd) < headerSize)
    {
      return false;
    }

    // Try to parse an entry header at this location in the byte stream
    PayloadEntryHeader header;
    It valueBegin;
    tie(header, valueBegin) =
      Deserialize<PayloadEntryHeader>::fromNetworkByteStream(bsBegin, bsEnd);

    // Ensure that the reported size of the entry does not exceed the
    // length of the byte stream. header.size comes off the wire, so it is
    // compared against the bytes left before any iterator is formed from it.
    if (header.size > static_cast<std::uint64_t>(distance(valueBegin, bsEnd)))
    {
      return false;
    }
    It valueEnd = valueBegin + static_cast<ItDiff>(header.size);

    // The next entry will start at the end of this one
    bsBegin = valueEnd;

    // Use the appropriate handler for this entry, if available
    auto handlerIt = map.find(header.key);
    if (handlerIt != end(map))
    {
      if (!handlerIt->second(std::move(valueBegin), std::move(valueEnd)))
      {
        return false;
      }
    }
  }
  return true;
}

} // namespace detail


// Payload encoding
template <typename... Entries>
struct Payload;

template <typename First, typename Rest>
struct Payload<First, Rest>
{
  Payload(First first, Rest rest)
    : mFirst(std::move(first))
    , mRest(std::move(rest))
  {
  }

  Payload(PayloadEntry<First> first, Rest rest)
    : mFirst(std::move(first))
    , mRest(std::move(rest))
  {
  }

  template <typename RhsFirst, typename RhsRest>
  using PayloadSum =
    Payload<First, typename Rest::template PayloadSum<RhsFirst, RhsRest>>;

  // Concatenate payloads together into a single payload
  template <typename RhsFirst, typename RhsRest>
  friend PayloadSum<RhsFirst, RhsRest> operator+(Payload lhs,
                                                 Payload<RhsFirst, RhsRest> rhs)
  {
    return {std::move(lhs.mFirst), std::move(lhs.mRest) + std::move(rhs)};
  }

  friend std::size_t sizeInByteStream(const Payload& payload)
  {
    return sizeInByteStream(payload.mFirst) + sizeInByteStream(payload.mRest);
  }

  template <typename It>
  friend It toNetworkByteStream(const Payload& payload, It streamIt)
  {
    return toNetworkByteStream(
      payload.mRest, toNetworkByteStream(payload.mFirst, std::move(streamIt)));
  }

  PayloadEntry<First> mFirst;
  Rest mRest;
};

template <>
struct Payload<>
{
  template <typename RhsFirst, typename RhsRest>
  using PayloadSum = Payload<RhsFirst, RhsRest>;

  template <typename RhsFirst, typename RhsRest>
  friend PayloadSum<RhsFirst, RhsRest> operator+(Payload, Payload<RhsFirst, RhsRest> rhs)
  {
    return rhs;
  }

  friend std::size_t sizeInByteStream(const Payload&) { return 0; }

  template <typename It>
  friend It toNetworkByteStream(const Payload&, It streamIt)
  {
    return streamIt;
  }
};

template <typename... Entries>
struct PayloadBuilder;

// Payload factory function
template <typename... Entries>
auto makePayload(Entries... entries)
  -> decltype(PayloadBuilder<Entries...>{}(std::move(entries)...))
{
  return PayloadBuilder<Entries...>{}(std::move(entries)...);
}

template <typename First, typename... Rest>
struct PayloadBuilder<First, Rest...>
{
  auto operator()(First first, Rest... rest)
    -> Payload<First, decltype(makePayload(std::move(rest)...))>
  {
    return {std::move(first), makePayload(std::move(rest)...)};
  }
};

template <>
struct PayloadBuilder<>
{
  Payload<> operator()() { return {}; }
};

// Parse payloads to values
template <typename... Entries>
struct ParsePayload;

template <typename First, typename... Rest>
struct ParsePayload<First, Rest...>
{
  template <typename It, typename... Handlers>
  static bool parse(It begin, It end, Handlers... handlers)
  {
    detail::HandlerMap<It> map;
    collectHandlers(map, std::move(handlers)...);
    return detail::parseByteStream(map, std::move(begin), std::move(end));
  }

  template <typename It, typename FirstHandler, typename... RestHandlers>
  static void collectHandlers(detail::HandlerMap<It>& map,
                              FirstHandler handler,
                              RestHandlers... rest)
  {
    using namespace std;
    map[First::key] = [handler](const It begin, const It end)
    {
      // An entry is never encoded with an empty value.
      if (begin == end)
      {
        return false;
      }
      const auto res = First::fromNetworkByteStream(begin, end);
      // The entry did not consume exactly the bytes its header declared.
      if (res.second != end)
      {
        return false;
      }
      handler(res.first);
      return true;
    };

    ParsePayload<Rest...>::collectHandlers(map, std::move(rest)...);
  }
};

template <>
struct ParsePayload<>
{
  template <typename It>
  static void collectHandlers(detail::HandlerMap<It>&)
  {
  }
};

// False means the bytes were malformed and the message must be dropped:
// upstream reaches the same place by letting a range_error escape to the
// caller, which this fork cannot do.
template <typename... Entries, typename It, typename... Handlers>
bool parsePayload(It begin, It end, Handlers... handlers)
{
  using namespace std;
  return ParsePayload<Entries...>::parse(
    std::move(begin), std::move(end), std::move(handlers)...);
}

} // namespace discovery
} // namespace ableton
