defmodule CHORD.Main do
  require Logger
  @moduledoc """
  Creates chord ring based on number of nodes.
  Starts the search for the requested keys.
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
    numofReq = elem(inputs, 1)
    completedNodes = %{}
    nodes = createChordRing(noOfNodes, numofReq)
    {noOfNodes, numofReq, nodes, completedNodes}
  end

  defp createChordRing(noOfNodes, numofReq) do
    mbits = 12
    nodeIdTuple = getnodeIdTuple(noOfNodes, mbits)
    #nodeIdTuple = {"036", "05F", "2A4", "4A7", "5DD", "7DC", "832", "B8A", "BFD", "CFE", "D51","FBE"}
    Enum.each(0..noOfNodes-1, fn index->
      # Node id is present at currenct index and Successor is (next index)% noOfNodes in list
      nodeId = elem(nodeIdTuple,index)
      CHORD.NodeChord.start_link({nodeIdTuple, index, elem(nodeIdTuple,rem(index+1,noOfNodes)), mbits, numofReq}, nodeId)
    end)
    Logger.info("Initial Chord Ring Created")
    nodeIdTuple
    #initiateSearchKeysForAllNodes(nodeIdTuple)
  end

  defp initiateSearchKeysForAllNodes(nodeIdTuple) do
    Enum.each(Tuple.to_list(nodeIdTuple), fn nodeId ->
      GenServer.cast({:global, nodeId}, {:searchKeys})
    end)
  end

  defp findUniqueHash(mbits, nodesMap) do
    nodeId = :crypto.hash(:sha, createRandomIpAddress) |> Base.encode16
    # Take last m bits from the hash string
    nodeId = String.slice(nodeId, (String.length(nodeId) - div(mbits,4))..String.length(nodeId))
    nodesList =if (Map.has_key?(nodesMap,nodeId)) do
      findUniqueHash(mbits,nodesMap)
    else
      nodesMap = Map.put(nodesMap, nodeId, true)
      {nodesMap,nodeId}
    end
    nodesList
  end

  defp getnodeIdTuple(noOfNodes, mbits) do
    nodesMap = %{}
    nodeIdTuple = Enum.reduce(1..noOfNodes,{}, fn index, acc ->
      uniqueNodeId = findUniqueHash(mbits,nodesMap)
        nodesMap = elem(uniqueNodeId,0)
        acc = Tuple.append(acc, elem(uniqueNodeId,1))
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

  @doc """
  Starts searching for the keys
  """
  def handle_cast({:startQuery},{noOfNodes, numofReq, nodes, completedNodes}) do
    initiateSearchKeysForAllNodes(nodes)
    {:noreply,{noOfNodes, numofReq, nodes, completedNodes}}
  end

  @doc """
  Keeps track of the nodes which found the keys
  """
  def handle_cast({:completedReq, nodeId, avgHop}, {noOfNodes, numofReq, nodes, completedNodes}) do
    completedNodes = Map.put(completedNodes, nodeId, avgHop)
    if map_size(completedNodes) == noOfNodes do
      Logger.info("Completed chord search #{inspect(completedNodes)}")
    end
    {:noreply,{noOfNodes, numofReq, nodes, completedNodes}}
  end
end
