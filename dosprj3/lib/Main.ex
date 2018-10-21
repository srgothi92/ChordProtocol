defmodule CHORD.Main do
  require Logger
  @moduledoc """
  Creates topology and Transmits message or s,w
  based on the type of algorithm to random neighbours.
  """
  @doc """
  Starts the GenServer.
  """
  def start_link(noOfNodes, numofReq) do
    GenServer.start_link(__MODULE__, {noOfNodes, numofReq}, name: :genMain)
  end

  @doc """
  Initiates the state of the GenServer.
  """
  def init(inputs) do
    state = init_state(inputs)
    {:ok, state}
  end

  defp init_state(inputs) do
    noOfNodes = elem(inputs, 0) || 5
    nodes = {}
    numofReq = elem(inputs, 1)
    completedNodes = %{}
    createChordRing(noOfNodes)
    {noOfNodes, numofReq, nodes, completedNodes}
  end

  defp createChordRing(noOfNodes) do
    mbits = 12
    nodeIdTuple = getnodeIdTuple(noOfNodes, mbits)
    Enum.each(0..noOfNodes-1, fn index->
      # Node id is present at currenct index and Successor is (next index)% noOfNodes in list
      nodeId = elem(nodeIdTuple,index)
      #nodeIdTuple = {"396","458","4DE","63E","762","82A","92E","954","AEF","CD3","ED8","EF5"}
      CHORD.NodeChord.start_link({nodeIdTuple, index, elem(nodeIdTuple,rem(index+1,noOfNodes)), mbits}, nodeId)
    end)
  end

  defp getnodeIdTuple(noOfNodes, mbits) do
    nodeIdTuple = Enum.reduce(1..noOfNodes,{}, fn index, acc ->
      nodeId = :crypto.hash(:sha, createRandomIpAddress) |> Base.encode16
      # Take last m bits from the hash string
      nodeId = String.slice(nodeId, (String.length(nodeId) - div(mbits,4))..String.length(nodeId))
      acc = Tuple.append(acc, nodeId)
    end)
    nodeIdList = Tuple.to_list(nodeIdTuple)
    nodeIdList = Enum.sort(nodeIdList)
    nodeIdTuple = List.to_tuple(nodeIdList)
    IO.inspect nodeIdTuple
    nodeIdTuple
  end

  defp createRandomIpAddress() do
    aIpAddr = [:rand.uniform(255), :rand.uniform(255),:rand.uniform(255),:rand.uniform(255)]
    aIpAddr |> Enum.join(":")
  end

  def handle_call({:completedReq, nodeId, avgHop}, {noOfNodes, numofReq, nodes, completedNodes}) do
    completedNodes = Map.put(nodeId, avgHop)
    if map_size(completedNodes) == noOfNodes do
      Logger.info("Completed chord search #{inspect(completedNodes)}")
    end
  end
end
