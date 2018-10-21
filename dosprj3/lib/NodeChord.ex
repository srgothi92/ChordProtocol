defmodule CHORD.NodeChord do
  use GenServer
  require Logger

  @doc """
  Starts the GenServer.
  """
  def start_link(inputs, name) do
    GenServer.start_link(__MODULE__, inputs, name: {:global, name})
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
    mbits = elem(inputs, 3)
    successor = elem(inputs, 2)
    nodeIdList = elem(inputs, 0)
    nodeIndex = elem(inputs, 1)
    nodeId = elem(nodeIdList, nodeIndex)
    key = nodeId
    fingerTable = initializeFingerTable(nodeId, mbits, nodeIdList, nodeIndex)
    completedReqHopCount = []
    # GenServer.call(self(), {:stabilize, nodeId})
    # GenServer.call(self(), {:fixFingerTable})
    {fingerTable, predecessor, successor, mbits, nodeId, key, completedReqHopCount}
  end

  def initializeFingerTable(nodeId, mbits, nodeIdList, nodeIndex) do
    fingerTable = Enum.reduce(1..(mbits - 1), %{}, fn index, acc ->
      # Convert hex string to integer and add 2 pow i
      newNodeId =
        rem(
          elem(Integer.parse(nodeId, 16), 0) + Kernel.trunc(:math.pow(2, index)),
          Kernel.trunc(:math.pow(2, mbits))
        )

      # Convert Integer back to hex
      newNodeId = Integer.to_string(newNodeId, 16)
      nextNode = findFingerNode(nodeIdList, nodeIndex, newNodeId)
      Map.put(acc, index, nextNode)
    end)
    IO.inspect(fingerTable)
    fingerTable
  end

  defp findFingerNode(nodeList, index, key) do
    fingerNodeId =
      cond do
        index == 0 && key > elem(nodeList, tuple_size(nodeList)-1)->
          elem(nodeList, 0)

        index != 0 && index < tuple_size(nodeList) && key > elem(nodeList, index-1) && key <  elem(nodeList, index) ->
          elem(nodeList, index)

        true ->
          index = rem(index+1, tuple_size(nodeList))
          findFingerNode(nodeList, index, key)
      end

    fingerNodeId
  end

  def handle_cast(
        {:searchKeys, numofReq},
        {fingerTable, predecessor, successor, mbits, nodeId, key, completedReqHopCount}
      ) do
    Enum.reduce(0..(numofReq - 1), fn _ ->
      randomKey = generateRandomKey(mbits)
      GenServer.call(self(), {:findSuccessor, randomKey})
    end)

    {:noreply, {fingerTable, predecessor, successor, mbits, nodeId, key, completedReqHopCount}}
  end

  defp generateRandomKey(mbits) do
    aIpAddr = [:rand.uniform(255), :rand.uniform(255), :rand.uniform(255), :rand.uniform(255)]
    hashKey = aIpAddr |> Enum.join(":")
    hashKey = :crypto.hash(:sha, hashKey) |> Base.encode16()

    hashKey =
      String.slice(hashKey, (String.length(hashKey) - rem(mbits, 8))..String.length(hashKey))

    hashKey
  end

  def handle_cast(
        {:fixFingerTable},
        {fingerTable, predecessor, successor, mbits, nodeId, key, completedReqHopCount}
      ) do
    Enum.each(0..(mbits - 1), fn index ->
      newSuccessor = GenServer.call(self(), {:findSuccessor, index})
      Map.put(fingerTable, index, newSuccessor)
    end)

    Process.send_after(self(), :fixFingerTable, 100)
    {:noreply, {fingerTable, predecessor, successor, mbits, nodeId, key, completedReqHopCount}}
  end

  def handle_cast(
        {:join, knownNode},
        {fingerTable, predecessor, _, mbits, nodeId, key, completedReqHopCount}
      ) do
    successor = GenServer.call(knownNode, {:findFingerNode, nodeId})
    GenServer.call(self(), {:notify, successor})
    {:noreply, {fingerTable, predecessor, successor, mbits, nodeId, key, completedReqHopCount}}
  end

  def handle_cast(
        {:notify, newPredecessor},
        {fingerTable, _, successor, mbits, nodeId, key, completedReqHopCount}
      ) do
    {:noreply, {fingerTable, newPredecessor, successor, mbits, nodeId, key, completedReqHopCount}}
  end

  def handle_call(
        {:findSuccessor, nodeIdToFind, startingNode, hopCount},
        {fingerTable, predecessor, successor, mbits, nodeId, key, completedReqHopCount}
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

    {:reply, foundSuccessor,
     {fingerTable, predecessor, successor, mbits, nodeId, key, completedReqHopCount}}
  end

  def handle_cast(
        {:searchCompleted, hopCount},
        {fingerTable, predecessor, successor, mbits, nodeId, key, completedReqHopCount}
      ) do
    completedReqHopCount = completedReqHopCount ++ [hopCount]
    GenServer.call(self(), {:searchKeys, hopCount})
    {:noreply, {fingerTable, predecessor, successor, mbits, nodeId, key, completedReqHopCount}}
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
        {fingerTable, predecessor, successor, mbits, nodeId, key, completedReqHopCount}
      ) do
    {:reply, predecessor,
     {fingerTable, predecessor, successor, mbits, nodeId, key, completedReqHopCount}}
  end

  def handle_cast(
        {:stabilize, nodeId},
        {fingerTable, predecessor, successor, mbits, nodeId, key, completedReqHopCount}
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
    {:noreply, {fingerTable, predecessor, successor, mbits, nodeId, key, completedReqHopCount}}
  end
end
