defmodule RingCache do
  require Logger

  @moduledoc """
  A time deterministic cache implemented as timed expiring rings

  Resolver:
  The resolver receives a list of keys and must return a list of lists
  where the interior list is [ key, value ], a nil value will result in
  a cached negative value that will have the same lifespan as a value.


  A simultaneous call for a single cell will likely result in the
  resolver being called more than once because of the delay introduced by
  the GenServer updating the :ets table.

  This module can be `use`d in another module to add cache_ functions and
  all RingCache functionality.

  A simple example that uses your filesystem as a source:


  iex> { :ok, pid } = RingCache.start(:test,
  ...>  fn questions -> Enum.map( questions, fn q1 -> [ q1,File.ls(q1) ] end ) end )
  iex> is_pid( pid )
  true
  iex> RingCache.cell_get( "config", :test )
  { :ok, [ "config.exs" ] }
  iex> RingCache.cell_get( "mix.exs", :test )
  { :error, :enotdir }
  iex> Process.sleep(500) # This gives the GenServer time to update the table
  iex> RingCache.inspect_order( :test )
  [{1, :test1}, {2, :test2}, {3, :test3}]
  iex> RingCache.inspect_tables( :test ) |> IO.inspect
  [
  test1: [],
  test2: [],
  test3: [{"mix.exs", {:error, :enotdir}}, {"config", {:ok, ["config.exs"]}}]
  ]
  iex> GenServer.stop( pid )
  :ok
  """
  
  @tabcount 3
  @tabexpire_ms 5 * 60 * 1000
  @default_opts [
    tabcount: @tabcount,
		tabexpire_ms: @tabexpire_ms,
	]

  @type cell_key_t :: ( String.t | tuple )
  @type cell_value_t :: ( :negative_cache | map() | String.t | tuple )
  @type cell_tuple_t :: { cell_key_t , cell_value_t }
  @type kv_t :: { any(), any() }
  @type resolver_t :: ( ( [ any ] ) -> [ kv_t, ... ] | mfa() )
  @type rc_name_t :: ( atom | String.t )
  
  @spec start( rc_name :: rc_name_t, resolver :: resolver_t, opts :: keyword() ) :: { :ok, pid() }
  def start( rc_name, resolver, opts \\ []) do
    opts = Keyword.merge( @default_opts, opts )
    GenServer.start( RingCache.Impl, [rc_name, resolver, opts], name: rc_name)
  end

  @spec start_link( rc_name :: rc_name_t, resolver :: resolver_t, opts :: keyword() ) :: { :ok, pid() }
  def start_link( rc_name, resolver, opts \\ []) do
    opts = Keyword.merge( @default_opts, opts )
    GenServer.start_link( RingCache.Impl, [rc_name, resolver, opts], name: rc_name)
  end

  def cell_get( cell_key, rc_name ) when is_list( cell_key ) do
    get_and_resolve_keys( cell_key, rc_name )
    |> Enum.map( fn
      { _mid, :negative_cache } -> nil
      { _mid, val } -> val
    end)
  end

  def cell_get( cell_key, rc_name ) do
    case get_and_resolve_key( cell_key, rc_name ) do
      { _mid, :negative_cache } -> nil
      { _mid, val } -> val
    end
  end

  def cell_get_tuples( cell_key, rc_name ) when is_list( cell_key ) do
    get_and_resolve_keys( cell_key, rc_name )
    |> Enum.map(fn
      { k, :negative_cache } -> { k, nil }
      kv -> kv
    end)
  end
  
  def cell_get_tuples( cell_key, rc_name ) do
    get_and_resolve_keys( [ cell_key ], rc_name )
    |> Map.get( cell_key )
    |> case do
	       :negative_cache -> { cell_key, nil }
	       val -> { cell_key, val }
       end
  end

  # Private or internal below

  defp resolve_keys( cell_key, rc_name ) do
    case get_resolver( rc_name ) do
      { m, f, a } -> apply( m, f, [ cell_key | a ] )
      fun when is_function( fun ) -> apply( fun, [ cell_key ] )
    end
    |> Enum.map( fn
      { mid, nil } -> { mid, :negative_cache }
      { mid, mm } -> { mid, mm }
      [ mid, nil ] -> { mid, :negative_cache }
      [ mid, mm ] -> { mid, mm }
    end )
    |> insert_cells( rc_name )
  end

  defp get_and_resolve_key( cell_key, rc_name ) do
    get_and_resolve_keys( [ cell_key ], rc_name )
    |> Map.to_list
    |> hd
  end

  defp get_and_resolve_keys( cell_key, rc_name ) do
    get_keys( cell_key, rc_name )
    |> get_unresolved( rc_name )
  end

  # return keys that are located in the local tables
  
  defp get_keys( cell_keys, rc_name ) do
    get_keys( cell_keys, tab_first( rc_name ), [], rc_name )
  end
  
  defp get_keys( cell_keys, :"$end_of_table", resolved, _rc_name ) do
    %{
      resolved: Map.new( resolved ),
      unresolved: cell_keys
    }
  end
  
  defp get_keys( cell_keys, tabid, resolved, rc_name ) do
    tabname = get_tabname( tabid, rc_name )
    cell_keys
    |> Enum.reduce(
      [ unresolved: [], resolved: resolved ], fn
      mid, [ unresolved: u, resolved: r ] ->
	      case :ets.lookup( tabname, mid ) do
	        [] -> [ unresolved: [ mid | u ], resolved: r ]
	        [{ mid, val }] -> [ unresolved: u, resolved: [ { mid, val } | r ] ]
	      end
    end)
    |> case do
	       [ unresolved: [], resolved: r ] -> get_keys( [], :"$end_of_table", r, rc_name )
	       [ unresolved: u, resolved: r ] -> get_keys( u, tab_next( tabid, rc_name ), r, rc_name )
       end
  end

  defp get_unresolved( %{ resolved: res, unresolved: [] }, _rc_name ), do: res
  
  defp get_unresolved( %{ resolved: res, unresolved: unr }, rc_name ) do
    Map.merge( res, Map.new( resolve_keys( unr, rc_name ) ) )
  end

  defp tab_first( rc_name ), do: :ets.first( order_name( rc_name ) )
  defp tab_next( after_tab, rc_name ), do: :ets.next( order_name( rc_name ), after_tab )

  # this is duplicated due to compilation dependencies
  def get_tabname( tabid, rc_name ) do
    [ { _tabid, tabname } ] = :ets.lookup( order_name( rc_name ), tabid )
    tabname
  end

  def order_name( rc_name ) do
    String.to_atom(to_string( rc_name ) <> "_order")
  end
  
  def inspect_order( rc_name ) do
    :ets.tab2list( order_name( rc_name ) )
  end
  
  def inspect_tables( rc_name ) do
    Enum.map( :ets.tab2list( order_name( rc_name ) ), fn
      { _, tabname } -> { tabname, :ets.tab2list( tabname )}
    end )
  end
  
  def clear( rc_name ) do
    GenServer.cast( rc_name, {:clear})
  end

  def insert_cells( new_cells, rc_name ) do
    GenServer.cast( rc_name, { :insert_cells, new_cells } )
    new_cells
  end

  def delete_cell( cell_key, rc_name ) do
    GenServer.cast( rc_name, { :delete_cell, cell_key } )
  end

  def expire_table( rc_name )  do
    GenServer.cast( rc_name, :expire_table )
  end

  def get_resolver( rc_name ) do
    GenServer.call( rc_name, :get_resolver )
  end

  @spec set_resolver( resolver :: resolver_t, rc_name :: rc_name_t ) :: :ok
  def set_resolver( resolver, rc_name ) do
    GenServer.cast( rc_name, { :set_resolver, resolver } )
  end
  # server implementation

  defmodule Impl do
    use GenServer

    def init( [ rc_name, resolver, opts] ) do
      Process.flag(:trap_exit, true)

      order_key = order_name( rc_name )
      :ets.new( order_key, [ :ordered_set, :named_table ] )

      options = [ :set, :named_table ]
      
      for n <- (1..( opts[:tabcount] )) do
	      name = String.to_atom( to_string(rc_name) <> to_string(n) )
	      :ets.new( name, options )
	      :ets.insert( order_key, { n, name } )
	      name
      end

      opts =
	      opts
	    |> Keyword.put( :expire_timer,
	      :timer.apply_interval( opts[:tabexpire_ms], RingCache, :expire_table, [ rc_name ] ) )
      
      {:ok, { rc_name, resolver, order_key, opts } }
    end
    
    def terminate(reason, _state) do
      reason
    end

    def order_name( rc_name ) do
      String.to_atom(to_string( rc_name ) <> "_order")
    end
    
    def get_tabname( tabid, order_key ) do
      [ { _tabid, tabname } ] = :ets.lookup( order_key, tabid )
      tabname
    end
    
    def get_insert_tabname( order_key ) do
      get_tabname( :ets.last( order_key ), order_key )
    end

    def handle_call( :get_resolver, _from, { rc_name, resolver, order_key, opts } ) do
      { :reply, resolver, { rc_name, resolver, order_key, opts } }
    end

    def handle_cast( { :set_resolver, new_resolver }, { rc_name, _resolver, order_key, opts } ) do
      { :noreply, { rc_name, new_resolver, order_key, opts } }
    end
    
    def handle_cast( { :insert_cells, cells}, { rc_name, resolver, order_key, opts } ) when is_list( cells ) do
      :ets.insert( get_insert_tabname( order_key ), cells )
      { :noreply, { rc_name, resolver, order_key, opts } }
    end
    
    def handle_cast( { :insert_cells, { _key, _val } = mtup }, { rc_name, resolver, order_key, opts } ) do
      :ets.insert( get_insert_tabname( rc_name ), mtup )
      { :noreply, { rc_name, resolver, order_key, opts } }
    end

    def handle_cast( { :delete_cell, cell_key }, { rc_name, resolver, order_key, opts } ) do
      :ets.tab2list( order_key )
      |> Enum.map( fn { _key, tabname } -> :ets.delete( tabname, cell_key ) end )
      { :noreply, { rc_name, resolver, order_key, opts } }
    end

    def handle_cast( { :clear }, { rc_name, resolver, order_key, opts } ) do
      :ets.tab2list( order_key )
      |> Enum.map( fn { _key, tabname } -> :ets.delete_all_objects( tabname ) end )
      { :noreply, { rc_name, resolver, order_key, opts } }
    end

    def handle_cast( :expire_table, { rc_name, resolver, order_key, opts } ) do
      target_key = :ets.first( order_key )
      target_tab = get_tabname( target_key, order_key )
      Logger.debug("RingCache(#{rc_name}):expire_table expiring: #{target_tab} size:#{:ets.info(target_tab)[:size]}  first:#{:ets.first( order_key ) } last:#{:ets.last( order_key ) }")

      :ets.delete( order_key, target_key )
      :ets.delete_all_objects( target_tab )
      :ets.insert( order_key, { :ets.last( order_key ) + 1, target_tab } )
      { :noreply, { rc_name, resolver, order_key, opts } }
    end

    def handle_info( :EXIT, _from, :normal ) do
      exit(:normal)
    end
    
  end

  @doc """
  Add cache_ functions to a module for a module wrapped ring cache.
  Synonymous fns are:
  cache_start
  cache_get
  cache_get_tuples
  cache_inspect_tables
  cache_inspect_order
  cache_clear
  cache_get_resolver
  cache_set_resolver
  """
  
  def instrument do
    quote do
      def cache_start( resolver, opts \\ [] ) do
	      RingCache.start( __MODULE__, resolver, opts )
      end
      def cache_start_link( resolver, opts \\ [] ) do
	      RingCache.start_link( __MODULE__, resolver, opts )
      end
      def cache_get( key ) do
	      RingCache.cell_get( key, __MODULE__ )
      end
      def cache_get_tuples( key ) do
	      RingCache.cell_get_tuples( key, __MODULE__ )
      end
      def cache_inspect_tables, do: RingCache.inspect_tables( __MODULE__ )
      def cache_inspect_order, do: RingCache.inspect_order( __MODULE__ )
      def cache_clear, do: RingCache.clear( __MODULE__ )
      def cache_get_resolver, do: RingCache.get_resolver( __MODULE__ )
      def cache_set_resolver( new_resolver ), do: RingCache.set_resolver( new_resolver,  __MODULE__ )
      def cache_delete( key ), do: RingCache.delete_cell( key, __MODULE__ )
    end
  end
  
  defmacro __using__( _opts \\ [] ) do
    instrument()
  end
end
