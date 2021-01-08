defmodule RingCacheTest do
  use ExUnit.Case
  doctest RingCache

  test "expire logging" do
    Process.put(:ct, 0)
    assert { :ok, _pid } = RingCache.start( :test2,
      fn questions ->
	Enum.map( questions, fn q1 ->
	  p1 = Process.get(:ct)
	  IO.puts( "question: #{q1}, ct #{p1}" )
	  Process.put(:ct, p1+1)
	  [ q1, "#{q1} answer #{p1}" ]
	end )
      end,
      [
	log_expire: true,
	tabexpire_ms: 1000,
      ]
    )

    assert "foo answer 0" = RingCache.cell_get( "foo", :test2 )
    assert :ok = RingCache.after_update( :test2 )
    assert "foo answer 0" = RingCache.cell_get( "foo", :test2 )
    assert :ok = RingCache.after_update( :test2 )
    # Ensure the 3 rings have expired, forces recalculation of foo
    Process.sleep( 5_100)
    assert "foo answer 1" = RingCache.cell_get( "foo", :test2 )

  end
end
