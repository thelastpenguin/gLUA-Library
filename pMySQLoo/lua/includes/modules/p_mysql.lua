if( not SERVER )then return end
print("-- LOADED pMySQL by thelastpenguin™");

pMySQL = {};

--
-- Load MySQLoo
--

require( 'mysqloo' );

if( not mysqloo )then
	ErrorNoHalt( 'pMySQL Lib Failed to load. MODULE MySQLoo NOT FOUND!');
	hook.Add( 'PlayerInitialSpawn', function( ply )
		timer.Simple( 1, function( ply )
			if( IsValid( ply ) and ply:IsAdmin() )then
				ply:ChatPrint('FAILED TO LOAD pMySQL Lib. MySQLoo not found!');
			end
		end);
	end);
	return;
end

--
-- DATABASE
-- 

local table = table;
local setmetatable = setmetatable;
local tostring = tostring;
local tonumber = tonumber;
local mysqloo = mysqloo;
local string = string;
local CurTime = CurTime;
local unpack = unpack;
local PrintTable = PrintTable;
local SortedPairs = SortedPairs;
local error, ErrorNoHalt = error, ErrorNoHalt;
local print = print;


module( "pMySQL", package.seeall )

version = 2;

local database_mt = {};
database_mt.__index = database_mt;

function database_mt:Init( host, user, pass, dbname, port )
	print("ATTEMPTING DB CONNECTION!");
	self.queue = {};


	self.host = host or self.host;
	self.user = user or self.user;
	self.pass = pass or self.pass;
	self.dbname = dbname or self.dbname;
	self.port = port or self.port or 3306;


	local pself = self;
	local port = self.port;

	local db = mysqloo.connect( self.host, self.user, self.pass, self.dbname, self.port );
	self.db = db;

	function db.onConnected( dbself )
		print( "[pMySQL] Successfully Connected to DB:" )
		print( "   ", self.host..':'..self.port );
		print( "   ", self.user );
		print( "   ", self.pass );
		print( "   ", self.dbname );

		print("EXECUTING ".. table.Count( self.queue ).." QUEUED QUERIES.");
		for k,v in SortedPairs( self.queue )do
			v:_Execute( ); 
		end
		table.Empty( self.queue );
	end

	function db.onConnectionFailed( dbself, err )
		pself.status = CONNECTION_FAILED;
		print( "[pMySQL] FAILED TO CONNECT TO DATABASE:" )
		print( "   ", self.host..':'..self.port );
		print( "   ", self.user );
		print( "   ", self.pass );
		print( "   ", self.dbname );
		print( "Error:", err )
	end

	db:connect( );

	return self;
end

function database_mt:SetHost( host )
	self.host = host;
	return self;
end
function database_mt:SetUser( user, pass )
	self.user = user;
	self.pass = pass or self.pass;
	return self;
end
function database_mt:SetPass( pass )
	self.pass = pass;
	return self;
end
function database_mt:SelectDB( name )
	self.dbname = name;
	return self;
end
function database_mt:SetPort( port )
	self.port = port;
	return self;
end

function database_mt:LoadAndCacheStatus( )
	if( CurTime() - self.db_status_t < 0.01 )then return ; end
	self.dbstatus = self.db:status();
	self.dbstatus_t = CurTime();
end

function database_mt:IsConnecting( )
	return self.db:status() == mysqloo.DATABASE_CONNECTING;
end
function database_mt:IsConnected( )
	return self.db:status() == mysqloo.DATABASE_CONNECTED;
end
function database_mt:NotConnected( )
	return self.db:status() ~= mysqloo.DATABASE_CONNECTED;
end

function database_mt:RunQuery( query )
	query:SetDatabase( self );
	query:_Execute( );

	return self;
end

function database_mt:SQLFormat( str, ... )
	local arg = {...};
	for k,v in pairs( arg )do
		arg[k] = self.db:escape( tostring( v ) );
	end
	return string.format( str:gsub( '?', '%%s' ), unpack( arg ) );
end

function database_mt:SQLStr( str )
	return self.db:escape( str );
end

function database_mt:NewQuery( )
	return newQuery( ):SetDatabase( self );
end

function newDB( )
	return setmetatable( {}, database_mt );
end







--
-- QUERY
-- 

function CALLBACK_PRINTTABLE( data, self )
	print("DATA FOR QUERY: ", self.str );
	PrintTable( data );
end

local query_mt = {};
query_mt.__index = query_mt;



local function query_onSuccess( self, data )
	if( self.cback )then
		self.cback( data, self );
	end
end

local function query_onError( self, error )
	if( self.db:NotConnected() )then
		if( not self.db:IsConnecting( ) )then
			print('[pMySQL] Query failed due to lost database connection. Reconnecting.');
			self.db.db:connect( );
		end

		print( '[pMySQL] Queued query to run later while reconnecting.');
		table.insert( self.db.queue, self )

		return ;
	end

	if( self.ehandler )then
		self.ehandler( error, self ); 
	else
		ErrorNoHalt('[pMySQL] ERROR ON QUERY!');
		print('    ', 'QUERY: ', self.str );
		print('    ', 'ERROR:\n  ', error );
	end
end

function query_mt:_Execute( )
	print("EXECUTING QUERY!");
	local q = self.db.db:query( self.str );
	if( not q )then
		query_onError( self, 'QUERY OBJECT FROM DB IS NIL' );
		return ;
	end
	print( 'QUERY: ', q );
	self.q = q;

	q.onSuccess = function( q, data )
		query_onSuccess( self, data );
	end
	q.onError = function( q, error )
		query_onError( self, error );
	end

	q:start( );

	return self;
end

function query_mt:SetDatabase( db )
	self.db = db;
	return self;
end
function query_mt:SetDB( db ) -- same thing essentially.
	self:SetDatabase( db )
	return self;
end 

function query_mt:SetString( str )
	self.str = str;
	return self;
end

function query_mt:SetSQL( str, ... )
	if( not self.db )then
		error("NO DB FOR SQL STRING PROCESSING! USE :SetDB first!");
		return self;
	end
	self.str = self.db:SQLFormat( str, ... )
	return self;
end

function query_mt:SetCallback( func )
	self.cback = func;

	return self;
end

function query_mt:SetErrorHandler( func )
	self.ehandler = func;

	return self;
end

function query_mt:Run( )
	if( not self.db )then
		error('QUERY DB NOT SET!');
		return ;
	end
	self.db:RunQuery( self );
end

query_mt.__call = function( self, ... )
	self:Run(...);
end


function newQuery( )
	return setmetatable( {}, query_mt );
end
