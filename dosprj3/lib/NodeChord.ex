defmodule CHORD.NodeChord do
  use GenServer
  require Logger

  @doc """
  Starts the GenServer.
  """
  def start_link(inputs, name) do
    GenServer.start_link(__MODULE__, inputs, name: name)
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
    GenServer.call(self(), {:stabilize, nodeId})
    GenServer.call(self(), {:fixFingerTable})
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
    node =
      if(elem(nodeList, index) > key) do
        elem(nodeList, index)
      else
        index = index + 1
        findFingerNode(nodeList, index, key)
      end

    node
  end

  def handle_cast(
        {:searchKeys, numofReq},
        {fingerTable, predecessor, successor, mbits, nodeId, key}
      ) do
    Enum.reduce(0..numofReq, fn _ ->
      randomKey = generateRandomKey(mbits)
      GenServer.call(self(), {:findSuccessor, randomKey})
    end)

    {:noreply, {fingerTable, predecessor, successor, mbits, nodeId, key}}
  end

  defp generateRandomKey(mbits) do
    aIpAddr = [:rand.uniform(255), :rand.uniform(255), :rand.uniform(255), :rand.uniform(255)]
    hashKey = aIpAddr |> Enum.join(":")
    hashKey = :crypto.hash(:sha, hashKey) |> Base.encode16()

    hashKey =
      String.slice(hashKey, (String.length(hashKey) - rem(mbits, 8))..String.length(hashKey))

    hashKey
  end

  def handle_cast({:fixFingerTable}, {fingerTable, predecessor, successor, mbits, nodeId, key}) do
    Enum.each(0..(mbits - 1), fn index ->
      newSuccessor = GenServer.call(self(), {:findSuccessor, index})
      Map.put(fingerTable, index, newSuccessor)
    end)

    Process.send_after(self(), :fixFingerTable, 100)
    {:noreply, {fingerTable, predecessor, successor, mbits, nodeId, key}}
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
        # Inform the node which started the search that key is found
        GenServer.call(startingNode, {:searchCompleted, hopCount})
        successor
      else
        hopCount = hopCount + 1
        nearesetNode = closestPrecedingNode(nodeIdToFind, mbits, fingerTable)
        # Query the successor Node for the key.
        GenServer.call(nearesetNode, {:findSuccessor, nodeIdToFind, startingNode, hopCount})
      end

    {:reply, foundSuccessor, {fingerTable, predecessor, successor, mbits, nodeId, key}}
  end

  def handle_call({:searchCompleted, hopCount}) do
    GenServer.call(self(), {:searchKeys, hopCount})
  end

  def closestPrecedingNode(nodeIdToFind, mbits, fingerTable) do
    foundNodeId =
      Enum.reduce_while(mbits..1, 0, fn mapKey, _ ->
        if Map.has_key?(fingerTable, mapKey) && nodeIdToFind > fingerTable.mapKey do
          {:halt, fingerTable.mapKey}
        else
          {:cont, fingerTable[mbits - 1]}
        end
      end)

    foundNodeId
  end

  def handle_call(
        {:getPredecessor},
        {fingerTable, predecessor, successor, mbits, nodeId, key}
      ) do
    {:reply, predecessor, {fingerTable, predecessor, successor, mbits, nodeId, key}}
  end

  def handle_cast(
        {:stabilize, nodeId},
        {fingerTable, predecessor, successor, mbits, nodeId, key}
      ) do
    # Get predecessor of successor
    nextNodePredecessor = GenServer.call(successor, {:getPredecessor})
    successor =
      Enum.reduce(1..mbits, 0, fn _, newSuccessor ->
        newSuccessor =
          if nextNodePredecessor >= nodeId && nextNodePredecessor <= successor do
            nextNodePredecessor
          else
            successor
          end
      end)

    Process.send_after(self(), :stabilize, 100)
    {:noreply, {fingerTable, predecessor, successor, mbits, nodeId, key}}
  end
end
