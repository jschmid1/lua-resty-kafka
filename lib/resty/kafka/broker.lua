-- Copyright (C) Dejiang Zhu(doujiang24)


local response = require "resty.kafka.response"

local ssl = require("ngx.ssl")

local to_int32 = response.to_int32
local setmetatable = setmetatable
local tcp = ngx.socket.tcp


local _M = {}
local mt = { __index = _M }


function _M.new(self, host, port, socket_config)
    return setmetatable({
        host = host,
        port = port,
        config = socket_config,
    }, mt)
end

function _M.send_receive(self, request)
    local sock, err = tcp()
    if not sock then
        return nil, err, true
    end

    sock:settimeout(self.config.socket_timeout)

    local ok, err = sock:connect(self.host, self.port)
    if not ok then
        return nil, err, true
    end
    if self.config.ssl then
        -- TODO: add reused_session for better performance of short-lived connections
        local opts = {
            ssl_verify = self.config.ssl_verify,
            client_cert = self.config.client_cert,
            client_priv_key = self.config.client_priv_key,
        }

        -- Read-in certificate

        local ssl = require "ngx.ssl"
        local f = assert(io.open("/certs/certchain.crt"))
        local cert_data = f:read("*a")
        f:close()

        local f = assert(io.open("/certs/privkey.key"))
        local key_data = f:read("*a")
        f:close()

        local cert_der, err = ssl.cert_pem_to_der(cert_data)
        if not cert_der then
            ngx.say("err -> " .. err)
            ngx.log(ngx.ERR, "failed to convert pem cert to der cert: ", err)
            return
        end

        local CERT, err = ssl.parse_pem_cert(cert_data)
        if not CERT then
            ngx_log(ERR, "error parsing cert: ", err)
            return nil, err
        end
        ngx.say("parsed certificate chain -> " .. tostring(CERT))

        local CERT_KEY, err = ssl.parse_pem_priv_key(key_data)
        if not CERT_KEY then
            ngx_log(ERR, "unable to parse cert key file: ", err)
            return nil, err
        end
        ngx.say("parsed private key -> " .. tostring(CERT_KEY))

        local ssl_params = {
            client_priv_key = CERT_KEY,
            client_cert = CERT,
        }       
        local _, err = sock:tlshandshake(ssl_params)
        if err then
            ngx.say(err)
            return nil, "failed to do SSL handshake with " ..
                        self.host .. ":" .. tostring(self.port) .. ": " .. err, true
        end
    end

    local bytes, err = sock:send(request:package())
    if not bytes then
        return nil, err, true
    end

    -- Reading a 4 byte `message_size`
    local data, err = sock:receive(4)
    
    if not data then
        if err == "timeout" then
            sock:close()
            return nil, err
        end
        return nil, err, true
    end

    local len = to_int32(data)

    local data, err = sock:receive(len)
    if not data then
        if err == "timeout" then
            sock:close()
            return nil, err
        end
        return nil, err, true
    end

    sock:setkeepalive(self.config.keepalive_timeout, self.config.keepalive_size)

    return response:new(data, request.api_version), nil, true
end


return _M
