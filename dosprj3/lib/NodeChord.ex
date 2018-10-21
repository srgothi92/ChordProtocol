defmodule CHORD.NodeChord do
  use GenServer
  require Logger

  @doc """
  Starts the GenServer.
  """
  def start_link(inputs, name) do
    GenServer.start_link(__MODULE__, inputs)
  end

  @doc """
  Initiates the state of the GenServer.
  """
  def init(inputs) do
    state = init_state(inputs)
    {:ok, state}
  end

  @doc """
  Initiates the message to empty string.
  Returns `{msg, neighbours, count}`
  """
  def init_state(inputs) do
    predecessor = nil
    mbits = elem(inputs, 2)
    successor = elem(inputs, 1)
    nodeIdList = elem(inputs, 3)
    nodeIndex = elem(inputs, 4)
    nodeId = elem(nodeIdList, nodeIndex)
    key = nodeId
    fingerTable = initializeFingerTable(nodeId, mbits, nodeIdList, nodeIndex)
    GenServer.call(self(),{:stabilize,nodeId})
    GenServer.call(self(),{:fixFingerTable})
    {fingerTable, predecessor, successor, mbits, nodeId, key}
  end

  def initializeFingerTable(nodeId, mbits, nodeIdList, nodeIndex) do
    Enum.reduce(0..(mbits - 1), %{}, fn index, fingerTable ->
      newNodeId = rem(nodeId + :math.pow(2, index), :math.pow(2, mbits))
      nextNode = findFingerNode(nodeIdList, nodeIndex, newNodeId)
      Map.put(fingerTable, index, nextNode)
    end)
  end

  defp findFingerNode(nodeList, index, key) do
    node = if(elem(nodeList, index) >  key) do
      elem(nodeList, index)
    else
      index = index + 1
      findFingerNode(nodeList, index, key)
    end
    node
  end

  def handle_cast({:searchKeys},{fingerTable, predecessor, successor, mbits, nodeId, key}) do
    Enum.reduce(0..numofReq), fn index -> 
     randomKey = :rand.uniform(nodeIdList) 
     GenServer.call(self(), {:findSuccessor, randomKey})
     end
    {:noreply,{fingerTable, predecessor, successor, mbits, nodeId, key}} 
  end
  end

  def handle_cast({:fixFingerTable}, {fingerTable, predecessor, successor, mbits, nodeId, key}) do
    Enum.reduce(0..(mbits - 1), fn index,fingerTable ->
      GenServer.call(self(),{:findSuccessor,index})
    end
    Process.send_after(self(), :fixFingerTable, 100)
    {:noreply,{fingerTable, predecessor, successor, mbits, nodeId, key}}
  end

  def handle_cast({:join, knownNode}, {fingerTable, predecessor, _, mbits, nodeId, key}) do
    successor = GenServer.call(knownNode, {:findFingerNode, nodeId})
    GenServer.call(self(), {:notify, successor})
    {:noreply, {fingerTable, predecessor, successor, mbits, nodeId, key}}
  end

  def handle_cast({:notify, newPredecessor}, {fingerTable, _, successor, mbits, nodeId, key}) do
    {:noreply, {fingerTable, newPredecessor, successor, mbits, nodeId, key}}
  end

  def handle_call(
        {:findSuccessor, nodeIdToFind, startingNode, hopCount},
        {fingerTable, predecessor, successor, mbits, nodeId, key}
      ) do
    foundSuccessor =
      if nodeIdToFind > nodeId && nodeIdToFind < successor do
        successor
        # Inform the node which started the search that key is found
        GenServer.call(startingNode, {:searchCompleted, hopCount})
      else
        hopCount = hopCount + 1
        nearesetNode = closestPrecedingNode(nodeIdToFind, mbits, fingerTable)
        # Query the successor Node for the key.
        GenServer.call(nearesetNode, {:findSuccessor, nodeIdToFind, startingNode, hopCount,})
      end
    {:reply, foundSuccessor, {fingerTable, predecessor, successor, mbits, nodeId, key}}
  end

  def handle_call({:searchCompleted, hopCount}) do
    GenServer.call(self(), {:searchKeys, hopCount})
  end

  def closestPrecedingNode(nodeIdToFind, mbits, fingerTable) do
    foundNodeId = Enum.reduce_while(mbits..1, 0,fn mapKey, _ ->
        if (Map.has_key?(fingerTable, mapKey) && nodeIdToFind > fingerTable.mapKey) do
          {:halt, fingerTable.mapKey}
        else
          {:cont, fingerTable[mbits-1]}
        end
    end)
    foundNodeId
  end

  def handle_call(
    {:getPredecessor}
    {fingerTable, predecessor, successor, mbits, nodeId, key}
  ) do
  {:reply,predecessor,{fingerTable, predecessor, successor, mbits, nodeId, key}}
  end

  def handle_cast(
    {:stabilize, nodeId}
    {fingerTable, predecessor, successor, mbits, nodeId, key}
  ) do
    nextNodePredecessor = GenServer.call(self(),{:getPredecessor})
    Enum.reduce(1..mbits, fn index ->
    successor = if ( nextNodePredecessor  >= nodeId && nextNodePredecessor  <= successor) do
      nextNodePredecessor 
    else
      successor
    end
    Process.send_after(self(), :stabilize, 100)
    {:noreply,{fingerTable, predecessor, successor, mbits, nodeId, key}}
  end

  def handle_call(
    {:countHops,}
  )

  
end
