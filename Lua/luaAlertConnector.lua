package.cpath = package.cpath .. ";" .. getWorkingFolder() .. "\\lib5" .. _VERSION:sub(_VERSION:len()) .. "\\?.dll";
con = require("QluaCSharpConnector_x64");
local json = require ("dkjson")


--====================================================================================================================
Stack = {}             -- Массив для стека сообщений 
Stack.idx_for_add = 1  -- Индекс для добавления следующей, или первой записи
Stack.idx_for_get = 1  -- Индекс для изъятия следующей, или первой записи
Stack.count = 0        -- Количество находящихся в стеке записей
Stack.max = 100         -- Максимально возможное количество записей в стеке (при переполнении старые записи будут замещаться новыми) 
--=====================================================================================================================

local functions = {}
IsRun = true;

--- Словарь открытых подписок (datasources) на свечи
data_sources = {}
last_indexes = {}

local is_connected = false
local lastTime = os.time()
local firstTime = lastTime
local timing = 10

--преобразуем строку json в таблицу
function from_json(str)
    local status, msg= pcall(json.decode, str, 1, json.null) -- dkjson
    if status then
        return msg
    else
        return nil, msg
    end
end

--преобразуем таблицу в строку json
function to_json(msg)
    local status, str= pcall(json.encode, msg, { indent = false }) -- dkjson
    if status then
        return str
    else
        error(str)
    end
end

-- Добавляет запись в стек
function addToStack(NewEntry)
   -- Добавляет запись в стек
   Stack[Stack.idx_for_add] = NewEntry;
   -- Корректирует счетчик находящихся в стеке записей
   if Stack.count < Stack.max then Stack.count = Stack.count + 1; end
   -- Увеличивает индекс для добавления следующей записи
   Stack.idx_for_add = Stack.idx_for_add + 1;
   -- Если индекс больше максимально допустимого, то следующая запись будет добавляться в начало стека
   if Stack.idx_for_add > Stack.max then Stack.idx_for_add = 1; end
   -- Если изъятие записей отстало от записи (новая запись переписала старую), то увеличивает индекс для изъятия следующей записи
   if Stack.idx_for_add - Stack.idx_for_get == 1 and Stack.count > 1 -- смещение внутри стека
      then Stack.idx_for_get = Stack.idx_for_get + 1;
   -- Добавил в конец, когда индекс для изъятия тоже был в конце и количество не равно 0
   elseif Stack.idx_for_get - Stack.idx_for_add == Stack.max - 1 and Stack.count > 1
      then Stack.idx_for_get = 1; 
   end
end

-- Извлекает запись из стека
function getFromStack()
   local OldInxForGet = Stack.idx_for_get;
   if Stack.count == 0 then return nil; end
   -- Уменьшает количество записей на 1
   Stack.count = Stack.count - 1
   -- Корректирует, если это была единственная запись
   if Stack.count == -1 then
      Stack.count = 0
      Stack.idx_for_get = Stack.idx_for_add -- Выравнивает индексы
   else -- Если еще есть записи 
      -- Сдвигает индекс изъятия на 1 вправо
      Stack.idx_for_get = Stack.idx_for_get + 1
      -- Корректирует, если достигнут конец
      if Stack.idx_for_get > Stack.max then Stack.idx_for_get = 1; end
   end;
   return Stack[OldInxForGet]
end;

function functions.dispatch_and_process(msg)
    if functions[msg.cmd] then
        local status, result = pcall(functions[msg.cmd], msg)
        if status then
            return result
        else
            msg.cmd = "lua_error"
            msg.lua_error = "Lua error: " .. result
            return msg
        end
    else
		msg.lua_error = "Command not implemented in Lua qsfunctions module: " .. msg.cmd
        msg.cmd = "lua_error"
        return msg
    end
end

function receiveRequest()
    if(con.CheckGotSend(socket.request.name, socket.request.len) ~= true) then
		err = " "
		local status, requestString= pcall(con.Read, "PL", 4000)
		if status and requestString then
			is_connected = true
			if string.sub(requestString, 1, 1) ~= "{" then
				requestString = ""
			end
			firstTime = os.time()
			local msg_table, err = from_json(requestString)
			if err then
				return nil, err
			else
				return msg_table
			end
		else
			return nil, err
		end
	else
		return nil
	end
end

function sendResponse(msg_table)
    local responseString = to_json(msg_table)
    if is_connected then
		addToStack(responseString..'\n')
    end
end

function main()
    local status, err = pcall(do_main)
    if status then
        log("finished")
		message("finished")
	end
end

function do_main()
	message('Start script luaAlertConnector')
	local requestMsg = ""
    while IsRun do
		lastTime = os.time()
        requestMsg = receiveRequest()
        if requestMsg then
		message(requestMsg)
            local responseMsg, err = functions.dispatch_and_process(requestMsg)
            if responseMsg then
                local res = sendResponse(responseMsg)
            end
        end
		
		if lastTime - firstTime >  timing and is_connected == true then
			is_connected = false
			Stack.idx_for_add = 1  
			Stack.idx_for_get = 1  
			Stack.count = 0
			for i = Stack.idx_for_add, Stack.max do
				Stack[i] = ""
			end
		end
		if is_connected then
			if StackResponse.count > 0 then
				if(con.CheckGotSend("LP", 4000) == true) then
					local mes_str = socket.getFromStack()
					local status, res = pcall(con.Write, "LP", 4000, mes_str .. "\n")
					if status and res then
						mes_str = ""
					end
				end
			end
		end
		sleep(1)
    end
end

function functions.start(msg)
	return msg
end

function create_data_source(msg)
	local class, sec, interval = get_candles_param(msg)
	local ds, error_descr = CreateDataSource(class, sec, interval)
	local is_error = false
	if(error_descr ~= nil) then
		msg.cmd = "lua_create_data_source_error"
		msg.lua_error = error_descr
		is_error = true
	elseif ds == nil then
		msg.cmd = "lua_create_data_source_error"
		msg.lua_error = "Can't create data source for " .. class .. ", " .. sec .. ", " .. tostring(interval)
		is_error = true
	end
	return ds, is_error
end

function fetch_candle(data_source, index)
	local candle = {}
	candle.low   = data_source:L(index)
	candle.close = data_source:C(index)
	candle.high = data_source:H(index)
	candle.open = data_source:O(index)
	candle.volume = data_source:V(index)
	candle.datetime = data_source:T(index)
	return candle
end

--- Подписаться на получения свечей по заданному инструмент и интервалу
function functions.subscribe_to_candles(msg)
	local ds, is_error = create_data_source(msg)
	if not is_error then
		local class, sec, interval = get_candles_param(msg)
		local key = get_key(class, sec, interval)
		data_sources[key] = ds
		last_indexes[key] = ds:Size()
		ds:SetUpdateCallback(
			function(index)
				data_source_callback(index, class, sec, interval)
			end)
	end
	return msg
end

function data_source_callback(index, class, sec, interval)
	local key = get_key(class, sec, interval)
	if index ~= last_indexes[key] then
		last_indexes[key] = index

		local candle = fetch_candle(data_sources[key], index - 1)
		candle.sec = sec
		candle.class = class
		candle.interval = interval

		local msg = {}
        msg.cmd = "NewCandle"
        msg.data = candle
        sendResponse(msg)
	end
end

--- Отписать от получения свечей по заданному инструменту и интервалу
function functions.unsubscribe_from_candles(msg)
	local class, sec, interval = get_candles_param(msg)
	local key = get_key(class, sec, interval)
	data_sources[key]:Close()
	data_sources[key] = nil
	last_indexes[key] = nil
	return msg
end

--- Проверить открыта ли подписка на заданный инструмент и интервал
function functions.is_subscribed(msg)
	local class, sec, interval = get_candles_param(msg)
	local key = get_key(class, sec, interval)
	for k, v in pairs(data_sources) do
		if key == k then
			msg.data = true;
			return  msg
		end
	end
	msg.data = false
	return msg
end

--- Возвращает из msg информацию о инструменте на который подписываемся и интервале
function get_candles_param(msg)
	local spl = split(msg.data, "|")
	return spl[1], spl[2], tonumber(spl[3])
end

--- Возвращает уникальный ключ для инструмента на который подписываемся и инетрвала
function get_key(class, sec, interval)
	return class .. "|" .. sec .. "|" .. tostring(interval)
end


function OnClose()
	if is_connected then
		local msg = {}
		msg.cmd = "OnClose"
		msg.data = ""
		socket.sendResponse(msg)
	end
	sleep(1000)
end

function OnStop(s)
	--if is_connected then
		local msg = {}
		msg.cmd = "OnStop"
		msg.data = s
		socket.sendResponse(msg)
	--end
	sleep(1000)
	IsRun = false;
    return 1000
end







