local client = require "resty.kafka.client"
local producer = require "resty.kafka.producer"
local ssl = require("ngx.ssl")
local ringbuffer = require "resty.kafka.ringbuffer"
local sendbuffer = require "resty.kafka.sendbuffer"
local request = require "resty.kafka.request"

local f = assert(io.open("/certs/certchain.crt"))
local cert_data = f:read("*a")
f:close()

-- FIXME: Although kafka is configured to setup topics on request, the first test will fail as the topic isn't created yet
-- The next request will work though. Maybe setup a `fake` request to all the topics used in this tests to set them up.

local TEST_TOPIC = "test"
local TEST_TOPIC_1 = "test1"
local key = "key"
local message = "message"

local CERT, err = ssl.parse_pem_cert(cert_data)

local f = assert(io.open("/certs/privkey.key"))
local key_data = f:read("*a")
f:close()

local CERT_KEY, err = ssl.parse_pem_priv_key(key_data)


local broker_list_sasl = {
    { host = "broker", port = 19093 },
}
local broker_list_sasl_ssl = {
    { host = "broker", port = 9093 },
}
local broker_list_mtls = {
    { host = "broker", port = 29093 },
}
local sasl_config = { mechanism="PLAIN",
                      user="admin",
                      password="admin-secret" }

local client_config_mtls = {
    ssl = true,
    client_cert = CERT,
    client_priv_key = CERT_KEY
}

local client_config_sasl_plain = {
    ssl = false,
    auth_config = sasl_config
}
local client_config_sasl_ssl = {
    ssl = true,
    client_cert = CERT,
    client_priv_key = CERT_KEY,
    auth_config = sasl_config
}

describe("Testing sasl client", function()

  before_each(function()
      cli = client:new(broker_list_sasl, client_config_sasl_plain)
  end)

  it("to build the metatable correctly", function()
    assert.are.equal(cli.socket_config.ssl, client_config_sasl_plain.ssl)
    assert.are.equal(cli.socket_config.ssl_verify, false)
    assert.are.equal(cli.auth_config.mechanism, sasl_config.mechanism)
    assert.are.equal(cli.auth_config.user, sasl_config.user)
    assert.are.equal(cli.auth_config.password, sasl_config.password)
  end)

  it("to fetch metadata correctly", function()
    -- Fetch metadata
    local brokers, partitions = cli:fetch_metadata(TEST_TOPIC)
    assert.are.same({{host = "broker", port = 19093}}, brokers)
    -- Check if return is as expected
    assert.are.same({{host = "broker", port = 19093}}, cli.brokers)
    -- Check if return was assigned to cli metatable
    assert.are.same({errcode = 0, id = 0, isr = {1}, leader = 1, replicas = {1}},partitions[0])
    -- Check if partitions were fetched correctly
    assert.is_not_nil(cli.topic_partitions[TEST_TOPIC])
    -- Check if cli partitions metatable was set correctly
  end)

  it("setup producers correctly", function()
    key = "foo"
    message = "bar"
    local p, err = producer:new(broker_list_sasl, client_config_sasl_plain)
    local offset, err = p:send("test", key, message)
    assert.is_nil(err)
    assert.is_number(tonumber(offset))
  end)
end)

describe("Testing sasl ssl client", function()

  before_each(function()
      cli = client:new(broker_list_sasl_ssl, client_config_sasl_ssl)
  end)

  it("to build the metatable correctly", function()
    assert.are.equal(cli.socket_config.ssl, client_config_sasl_ssl.ssl)
    assert.are.equal(cli.socket_config.ssl_verify, false)
    assert.are.equal(cli.auth_config.mechanism, sasl_config.mechanism)
    assert.are.equal(cli.auth_config.user, sasl_config.user)
    assert.are.equal(cli.auth_config.password, sasl_config.password)
  end)

  it("to fetch metadata correctly", function()
    -- Fetch metadata
    local brokers, partitions = cli:fetch_metadata(TEST_TOPIC)
    assert.are.same({{host = "broker", port = 9093}}, brokers)
    -- Check if return is as expected
    assert.are.same({{host = "broker", port = 9093}}, cli.brokers)
    -- Check if return was assigned to cli metatable
    assert.are.same({errcode = 0, id = 0, isr = {1}, leader = 1, replicas = {1}},partitions[0])
    -- Check if partitions were fetched correctly
    assert.is_not_nil(cli.topic_partitions[TEST_TOPIC])
    -- Check if cli partitions metatable was set correctly
  end)

  it("setup producers correctly", function()
    key = "foo"
    message = "bar"
    local p, err = producer:new(broker_list_sasl_ssl, client_config_sasl_ssl)
    local offset, err = p:send("test", key, message)
    assert.is_nil(err)
    assert.is_number(tonumber(offset))
  end)
end)

describe("Testing mtls client", function()

  before_each(function()
      cli = client:new(broker_list_mtls, client_config_mtls)
    end)

  it("to build the metatable correctly", function()
    assert.are.equal(cli.socket_config.ssl, client_config_mtls.ssl)
    assert.are.equal(cli.socket_config.ssl_verify, false)
    assert.are.equal(cli.socket_config.client_cert, CERT)
    assert.are.equal(cli.socket_config.client_priv_key, CERT_KEY)
  end)

  it("to fetch metadata correctly", function()
    -- Fetch metadata
    local brokers, partitions = cli:fetch_metadata(TEST_TOPIC)
    assert.are.same({{host = "broker", port = 29093}}, brokers)
    -- Check if return is as expected
    assert.are.same({{host = "broker", port = 29093}}, cli.brokers)
    -- Check if return was assigned to cli metatable
    assert.are.same({errcode = 0, id = 0, isr = {1}, leader = 1, replicas = {1}},partitions[0])
    -- Check if partitions were fetched correctly
    assert.is_not_nil(cli.topic_partitions[TEST_TOPIC])
    -- Check if cli partitions metatable was set correctly
  end)

  it("setup producers correctly", function()
    key = "foo"
    message = "bar"
    local p, err = producer:new(broker_list_mtls, client_config_mtls)
    local offset, err = p:send("test", key, message)
    assert.is_nil(err)
    assert.is_number(tonumber(offset))
  end)

end)

describe("Testing plain client", function()
  local broker_list_plain = {
      { host = "broker", port = 9092 },
  }

  before_each(function()
      cli = client:new(broker_list_plain)
    end)

  it("to build the metatable correctly", function()
    assert.are.equal(cli.socket_config.ssl, false)
    assert.are.equal(cli.socket_config.ssl_verify, false)
  end)

  it("to fetch metadata correctly", function()
    -- Fetch metadata
    local brokers, partitions = cli:fetch_metadata(TEST_TOPIC)
    assert.are.same({{host = "broker", port = 9092}}, brokers)
    -- Check if return is as expected
    assert.are.same({{host = "broker", port = 9092}}, cli.brokers)
    -- Check if return was assigned to cli metatable
    assert.are.same({errcode = 0, id = 0, isr = {1}, leader = 1, replicas = {1}},partitions[0])
    -- Check if partitions were fetched correctly
    assert.is_not_nil(cli.topic_partitions[TEST_TOPIC])
    -- Check if cli partitions metatable was set correctly
  end)

  it("setup producers correctly", function()
    local p, err = producer:new(broker_list_plain)
    local offset, err = p:send(TEST_TOPIC, key, message)
    assert.is_nil(err)
    assert.is_number(tonumber(offset))
  end)

  it("sends two messages and the offset is one apart", function()
    local p, err = producer:new(broker_list_plain)
    local offset1, err = p:send(TEST_TOPIC, key, message)
    local offset2, err = p:send(TEST_TOPIC, key, message)
    assert.is_nil(err)
    local diff = tonumber(offset2) - tonumber(offset1)
    assert.is.equal(diff, 1)
  end)

  it("sends two messages to two different topics", function()
    local p, err = producer:new(broker_list_plain)
    local offset1, err = p:send(TEST_TOPIC, key, message)
    assert.is_nil(err)
    assert.is_number(tonumber(offset1))
    local offset2, err = p:send(TEST_TOPIC_1, key, message)
    assert.is_nil(err)
    assert.is_number(tonumber(offset2))
  end)

  it("fails when topic_partitions are empty", function()
    local p, err = producer:new(broker_list_plain)
    p.client.topic_partitions.test = { [2] = { id = 2, leader = 0 }, [1] = { id = 1, leader = 0 }, [0] = { id = 0, leader = 0 }, num = 3 }
    local offset, err = p:send(TEST_TOPIC, key, message)
    assert.is_not_nil(err)
    assert.is_nil(offset)
    assert.is_same("not found broker", err)
  end)

  it("sends a lot of messages", function()
    local producer_config = { producer_type = "async", flush_time = 100}
    local p, err = producer:new(broker_list_plain, producer_config)
    -- init offset
    p:send(TEST_TOPIC, key, message)
    p:flush()
    local offset,_ = p:offset()
    local i = 0
    while i < 2000 do
          p:send(TEST_TOPIC, key, message..tostring(i))
          i = i + 1
    end
    ngx.sleep(0.2)
    local offset2, _ = p:offset()
    local diff = tostring(offset2 - offset)
    assert.is.equal(diff, "2000LL")
  end)

  it("test message buffering", function()
    local p = producer:new(broker_list_plain, { producer_type = "async", flush_time = 1000 })
    ngx.sleep(0.1) -- will have an immediately flush by timer_flush
    local size, err = p:send(TEST_TOPIC, key, message)
    assert.is_not_nil(size)
    assert.is_nil(err)
    ngx.sleep(1.1)
    local offset = p:offset()
    assert.is_true(tonumber(offset) > 0)
    p:flush()
    local offset0 = p:offset()

    local ok, err = p:send(TEST_TOPIC, key, message)
    assert.is_nil(err)
    assert.is_not_nil(ok)

    p:flush()
    local offset1 = p:offset()

    assert.is.equal(tonumber(offset1 - offset0), 1)
  end)

  it("timer flush", function()
    local p = producer:new(broker_list_plain, { producer_type = "async", flush_time = 1000 })
    ngx.sleep(0.1) -- will have an immediately flush by timer_flush

    local size, err = p:send(TEST_TOPIC, key, message)
    assert.is_not_nil(size)
    assert.is_nil(err)
    ngx.sleep(1.1)
    local offset = p:offset()
    assert.is_true(tonumber(offset) > 0)
  end)

  it("buffer flush", function()
    local p = producer:new(broker_list_plain, { producer_type = "async", batch_num = 1, flush_time = 10000})
    ngx.sleep(0.1) -- will have an immediately flush by timer_flush

    local ok, err = p:send(TEST_TOPIC, nil, message)
    assert.is_not_nil(ok)
    assert.is_nil(err)
    ngx.sleep(1)
    local offset0 = p:offset()
    p:flush()
    local offset1 = p:offset()
    local offset_diff = tonumber(offset1) - tonumber(offset0)
    assert.is.equal(offset_diff, 0)
  end)

  it("multi topic batch send", function()
    local p = producer:new(broker_list_plain, { producer_type = "async", flush_time = 10000})
    ngx.sleep(0.01)
    -- 2 message
    local size, err = p:send(TEST_TOPIC, key, message)
    assert.is_not_nil(size)
    assert.is_nil(err)
    local size, err = p:send(TEST_TOPIC_1, key, message)
    assert.is_not_nil(size)
    assert.is_nil(err)
    p:flush()
    local offset0 = p:offset()

    local size, err = p:send(TEST_TOPIC, key, message)
    assert.is_not_nil(size)
    assert.is_nil(err)
    local size, err = p:send(TEST_TOPIC_1, key, message)
    assert.is_not_nil(size)
    assert.is_nil(err)
    p:flush()

    local offset1 = p:offset()

    local offset_diff = tonumber(offset1 - offset0)
    assert.is.equal(offset_diff, 2)
  end)

  it("is not retryable ", function() 
    local p = producer:new(broker_list_plain, { producer_type = "async", flush_time = 10000})
    ngx.sleep(0.01)
    local size, err = p:send(TEST_TOPIC, key, message)
    assert.is_not_nil(size)
    assert.is_nil(err)
    p:flush()
    local offset0 = p:offset()

    p.sendbuffer.topics.test[0].retryable = false

    local size, err = p:send(TEST_TOPIC, key, message)
    assert.is_not_nil(size)
    assert.is_nil(err)
    p:flush()

    local offset1 = p:offset()
    local offset_diff = tonumber(offset1 - offset0)

    assert.is.equal(offset_diff, 1)
  end)

  it("sends in batches to two topics", function()
    local p = producer:new(broker_list_plain, { producer_type = "async", flush_time = 10000})
    ngx.sleep(0.01)
    -- 2 message
    local size, err = p:send(TEST_TOPIC, key, message)
    assert.is_not_nil(size)
    assert.is_nil(err)
    p:flush()
    local offset0 = p:offset()
    local size, err = p:send(TEST_TOPIC, key, message)
    assert.is_not_nil(size)
    assert.is_nil(err)
    local size, err = p:send(TEST_TOPIC, key, message)
    assert.is_not_nil(size)
    assert.is_nil(err)
    p:flush()

    local offset1 = p:offset()
    local offset_diff = tonumber(offset1 - offset0)
    assert.is.equal(offset_diff, 2)
  end)

  it("ringbuffers TODO", function()
    local buffer = ringbuffer:new(2, 3)

    local topic = "test"
    local key = "key"
    local message = "halo world"

    local ok, err = buffer:add(topic, key, message)
    assert.is_nil(err)
    assert.is_true(ok)
    assert.is_false(buffer:need_send())

    local ok, err = buffer:add(topic, key, message)
    assert.is_nil(err)
    assert.is_true(ok)
    assert.is_true(buffer:need_send())

    -- overflowing buffer
    buffer:add(topic, key, message)
    local ok, err = buffer:add(topic, key, message)
    assert.is_nil(ok)
    assert.not_nil(err)
    assert.is.equal(err, "buffer overflow")
    assert.is_true(buffer:need_send())
  end)

  it("pop buffer", function()
    local buffer = ringbuffer:new(2, 3)

    local base_key_buf_1 = "key"
    local base_message_buf_1 = "message"
    local base_key_buf_2 = "key"
    local base_message_buf_2 = "message"
    for i = 1, 2 do
      local key1 = base_key_buf_1 .. i
      local msg1 = base_message_buf_1 .. i
      local key2 = base_key_buf_2 .. i
      local msg2 = base_message_buf_2 .. i

      buffer:add(TEST_TOPIC, key1, msg1)
      buffer:add(TEST_TOPIC_1, key2, msg2)
      local topic_out, key_out, msg_out = buffer:pop()
      assert.is.equal(topic_out, TEST_TOPIC)
      assert.is.equal(key_out, key1)
      assert.is.equal(msg_out, msg1)
      local topic_out_2, key_out_2, msg_out_2 = buffer:pop()
      assert.is.equal(topic_out_2, TEST_TOPIC_1)
      assert.is.equal(key_out_2, key2)
      assert.is.equal(msg_out_2, msg2)
    end
  end)

  it("overflow sendbuffer", function()
    local buffer = sendbuffer:new(2, 20)

    local topic = "test"
    local partition_id = 1
    local key = "key"
    local message = "halo world"

    local overflow = buffer:add(topic, partition_id, key, message)
    assert.is_nil(overflow)

    local overflow = buffer:add(topic, partition_id, key, message)
    assert.is_true(overflow)
  end)

  it("offset?", function()
    local buffer = sendbuffer:new(2, 20)

    local topic = "test"
    local partition_id = 1
    local key = "key"
    local message = "halo world"

    local overflow = buffer:add(topic, partition_id, key, message)
    assert.is_nil(overflow)

    local offset = buffer:offset(topic, partition_id)
    assert.is.equal(0, offset)
    local offset = buffer:offset(topic, partition_id, 100)
    assert.is_nil(offset)
    local offset = buffer:offset(topic, partition_id)
    assert.is.equal(101, offset)
  end)

  it("verify if buffer clear works", function()
    local buffer = sendbuffer:new(2, 20)

    local topic = "test"
    local partition_id = 1
    local key = "key"
    local message = "halo world"

    local overflow = buffer:add(topic, partition_id, key, message)
    assert.is_nil(overflow)
    assert.is.equal(buffer.topics[topic][partition_id].used, 0)

    -- 1 item in the queue
    assert.is.equal(buffer.queue_num, 1)

    -- clearing buffer
    buffer:clear(topic, partition_id)

    assert.is_true(buffer:done())

    assert.is.equal(buffer.queue_num, 0)

    for i = 1, 10000 do
        buffer:clear(topic, partition_id)
    end

    assert.is.equal(buffer.topics[topic][partition_id].used, 1)
  end)

  it("test buffer:loop", function()
    local buffer = sendbuffer:new(2, 20)

    local topic = "test"
    local topic_2 = "test2"
    local partition_id = 1
    local key = "key"
    local message = "halo world"

    local overflow = buffer:add(topic, partition_id, key, message)
    local overflow = buffer:add(topic_2, partition_id, key, message)

    local res = {}
    for t, p in buffer:loop() do
      res[t] = p
    end
    assert.is.same(res, { test = 1, test2 = 1 })
  end)

end)

describe("Testing plain client with a bad broker in the bootstrap list", function()
  local broker_list_plain_bad_broker = {
      { host = "broker", port = 9999 },
      { host = "broker", port = 9092 }
  }
  before_each(function()
      cli = client:new(broker_list_plain_bad_broker)
    end)

  it("to build the metatable correctly", function()
    assert.are.equal(cli.socket_config.ssl, false)
    assert.are.equal(cli.socket_config.ssl_verify, false)
  end)

  it("to fetch metadata correctly and exclude the bad broker", function()
    -- Fetch metadata
    local brokers, partitions = cli:fetch_metadata(TEST_TOPIC)
    -- Expect the bad broker to not appear in this list
    assert.are.same({{host = "broker", port = 9092}}, brokers)
    -- Check if return is as expected
    assert.are.same({{host = "broker", port = 9092}}, cli.brokers)
    -- Check if return was assigned to cli metatable
    assert.are.same({errcode = 0, id = 0, isr = {1}, leader = 1, replicas = {1}},partitions[0])
    -- Check if partitions were fetched correctly
    assert.is_not_nil(cli.topic_partitions[TEST_TOPIC])
    -- Check if cli partitions metatable was set correctly
  end)

  it("setup producers correctly", function()
    local p, err = producer:new(broker_list_plain_bad_broker)
    local offset, err = p:send("test", key, message)
    assert.is_nil(err)
    assert.is_number(tonumber(offset))
  end)

end)


-- move to fixture dir or helper file
local function convert_to_hex(req)
    local str = req._req[#req._req]
    local ret = ""
    for i = 1, #str do
        ret = ret .. bit.tohex(string.byte(str, i), 2)
    end
    return ret
end

describe("test request lib TODO", function()
  -- TODO: test the new string, nullable_string and bytes as well

  it("test packing", function()
    local req = request:new(request.ProduceRequest, 1, "clientid")
    req:int16(-1 * math.pow(2, 15))
    local hex = convert_to_hex(req)
    assert.is.equal(hex, "8000")
    req:int16(math.pow(2, 15) - 1)
    local hex = convert_to_hex(req)
    assert.is.equal(hex, "7fff")
    req:int16(-1)
    local hex = convert_to_hex(req)
    assert.is.equal(hex, "ffff")
    req:int32(-1 * math.pow(2, 31))
    local hex = convert_to_hex(req)
    assert.is.equal(hex, "80000000")
    req:int32(math.pow(2, 31) - 1)
    local hex = convert_to_hex(req)
    assert.is.equal(hex, "7fffffff")
    req:int64(-1LL * math.pow(2, 32) * math.pow(2, 31))
    local hex = convert_to_hex(req)
    assert.is.equal(hex, "8000000000000000")
    req:int64(1ULL * math.pow(2, 32) * math.pow(2, 31) - 1)
    local hex = convert_to_hex(req)
    assert.is.equal(hex, "7fffffffffffffff")
  end)

end)
