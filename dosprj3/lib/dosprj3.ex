defmodule CHORD do
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
    mbits = 32
    nodeIdList = getNodeIdList(noOfNodes, mbits)
    Enum.each(0..noOfNodes-1, fn index->
      # Node id is present at currenct index and Successor is (next index)% noOfNodes in list
      nodeId = elem(nodeIdList,index)
      CHORD.NodeChord.start_link({nodeId, elem(nodeIdList,div(index+1,noOfNodes)), mbits}, name: nodeId)
    end)
  end

  defp getNodeIdList(noOfNodes, mbits) do
    nodeIdList = Enum.reduce(1..noOfNodes,{}, fn index, acc ->
      nodeId = :crypto.hash(:sha, createRandomIpAddress) |> Base.encode16
      nodeId = String.slice(nodeId, (String.length(nodeId) - rem(mbits,8))..String.length(nodeId))
      acc = Tuple.append(acc, nodeId)
    end)
    Enum.sort(nodeIdList)
  end

  defp createRandomIpAddress() do
    aIpAddr = [:rand.uniform(255), :rand.uniform(255),:rand.uniform(255),:rand.uniform(255)]
    aIpAddr |> Enum.join(":")
  end
end
