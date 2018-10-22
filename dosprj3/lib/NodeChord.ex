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
    noOfReq = elem(inputs, 4)
    nodeId = elem(nodeIdList, nodeIndex)
    key = nodeId
    fingerTable = initializeFingerTable(nodeId, mbits, nodeIdList, nodeIndex)
    completedReqHopCount = []
    # GenServer.call(self(), {:stabilize, nodeId})
    # GenServer.call(self(), {:fixFingerTable})
    {fingerTable, predecessor, successor, mbits, nodeId, key, completedReqHopCount, noOfReq}
  end

  defp convertToHex(num, mbits) do
    hexNumber = Integer.to_string(num, 16)
    # prepeing 0 to have similar ids
    difference = div(mbits, 4) - String.length(hexNumber)

    hex =
      if difference <= 0 do
        hexNumber
      else
        Enum.reduce(1..difference, hexNumber, fn _, acc -> "0" <> acc end)
      end

    hex
  end

  defp convertToInt(num) do
    elem(Integer.parse(num, 16), 0)
  end

  def initializeFingerTable(nodeId, mbits, nodeIdList, nodeIndex) do
    fingerTable =
      Enum.reduce(0..(mbits - 1), %{}, fn index, acc ->
        # Convert hex string to integer and add 2 pow i
        newNodeId =
          rem(
            convertToInt(nodeId) + Kernel.trunc(:math.pow(2, index)),
            Kernel.trunc(:math.pow(2, mbits))
          )

        nextNode = findFingerNode(nodeIdList, nodeIndex, newNodeId, mbits)
        Map.put(acc, index, nextNode)
      end)
    fingerTable
  end

  defp findFingerNode(nodeList, index, key, mbits) do
    previousNodeId =
      if index == 0 do
        # return the last node
        convertToInt(elem(nodeList, tuple_size(nodeList) - 1))
      else
        # return previous node
        convertToInt(elem(nodeList, index - 1))
      end

    # Logger.info("previousNodeId #{inspect(previousNodeId)}")
    currentNodeId = convertToInt(elem(nodeList, index))
    # Logger.info("currentNodeId #{inspect(currentNodeId)}")
    fingerNodeId =
      cond do
        index == 0 && (key > previousNodeId || key < currentNodeId) ->
          elem(nodeList, 0)

        index != 0 && index < tuple_size(nodeList) && key > previousNodeId && key <= currentNodeId ->
          convertToHex(currentNodeId, mbits)

        true ->
          index = rem(index + 1, tuple_size(nodeList))
          findFingerNode(nodeList, index, key, mbits)
      end

    fingerNodeId
  end

  def handle_cast(
        {:join, knownNode},
        {fingerTable, predecessor, _, mbits, nodeId, key, completedReqHopCount, noOfReq}
      ) do
    successor = GenServer.call(knownNode, {:findFingerNode, nodeId})
    GenServer.call(self(), {:notify, successor})

    {:noreply,
     {fingerTable, predecessor, successor, mbits, nodeId, key, completedReqHopCount, noOfReq}}
  end

  def handle_call(
        {:getPredecessor},
        {fingerTable, predecessor, successor, mbits, nodeId, key, completedReqHopCount, noOfReq}
      ) do
    {:reply, predecessor,
     {fingerTable, predecessor, successor, mbits, nodeId, key, completedReqHopCount, noOfReq}}
  end

  def handle_cast(
        {:notify, newPredecessor},
        {fingerTable, _, successor, mbits, nodeId, key, completedReqHopCount, noOfReq}
      ) do
    {:noreply,
     {fingerTable, newPredecessor, successor, mbits, nodeId, key, completedReqHopCount, noOfReq}}
  end

  def handle_cast(
        {:fixFingerTable},
        {fingerTable, predecessor, successor, mbits, nodeId, key, completedReqHopCount, noOfReq}
      ) do
    Enum.each(0..(mbits - 1), fn index ->
      GenServer.cast(self(), {:findSuccessor, fingerTable[index], nodeId, 0})
    end)

    Process.send_after(self(), :fixFingerTable, 100)

    {:noreply,
     {fingerTable, predecessor, successor, mbits, nodeId, key, completedReqHopCount, noOfReq}}
  end

  def handle_cast(
        {:stabilize, nodeId},
        {fingerTable, predecessor, successor, mbits, nodeId, key, completedReqHopCount, noOfReq}
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

    {:noreply,
     {fingerTable, predecessor, successor, mbits, nodeId, key, completedReqHopCount, noOfReq}}
  end

  def handle_cast(
        {:findSuccessor, nodeIdToFind, startingNode, hopCount},
        {fingerTable, predecessor, successor, mbits, nodeId, key, completedReqHopCount, noOfReq}
      ) do
    nodeIdToFindInt = convertToInt(nodeIdToFind)
    nodeIdInt = convertToInt(nodeId)
    successorInt = convertToInt(successor)
    findRelativeId = computeRelative(nodeIdToFindInt, nodeIdInt, mbits)
    successorRelativeId = computeRelative(successorInt, nodeIdInt, mbits)

    if findRelativeId > 0 && findRelativeId < successorRelativeId do
      # Inform the node which started the search that key is found
      GenServer.cast({:global, startingNode}, {:searchCompleted, successor, hopCount})
    else
      hopCount = hopCount + 1
      nearesetNode = closestPrecedingNode(nodeIdInt, nodeIdToFindInt, mbits, fingerTable)
      # Logger.info("NearesetNode: #{inspect(nearesetNode)}")
      # Query the successor Node for the key.
      GenServer.cast(
        {:global, nearesetNode},
        {:findSuccessor, nodeIdToFind, startingNode, hopCount}
      )
    end

    {:noreply,
     {fingerTable, predecessor, successor, mbits, nodeId, key, completedReqHopCount, noOfReq}}
  end

  def closestPrecedingNode(nodeIdInt, nodeIdToFindInt, mbits, fingerTable) do
    foundNodeId =
      Enum.reduce_while((mbits - 1)..0, 0, fn mapKey, _ ->
        if Map.has_key?(fingerTable, mapKey) do
          fingerNodeInt = convertToInt(fingerTable[mapKey])
          findIdRelative = computeRelative(nodeIdToFindInt, nodeIdInt, mbits)
          fingerIdRelative = computeRelative(fingerNodeInt, nodeIdInt, mbits)

          if fingerIdRelative > 0 && fingerIdRelative < findIdRelative do
            {:halt, fingerTable[mapKey]}
          else
            {:cont, fingerTable[mbits - 1]}
          end
        else
          {:cont, fingerTable[mbits - 1]}
        end
      end)

    foundNodeId
  end

  defp computeRelative(num1, num2, mbits) do
    ret = num1 - num2

    ret =
      if(ret < 0) do
        ret + Kernel.trunc(:math.pow(2, mbits))
      else
        ret
      end

    ret
  end

  def handle_cast(
        {:searchKeys},
        {fingerTable, predecessor, successor, mbits, nodeId, key, completedReqHopCount, noOfReq}
      ) do
    Enum.each(0..(noOfReq - 1), fn _ ->
      randomKey = generateRandomKey(mbits)
      GenServer.cast({:global, nodeId}, {:findSuccessor, randomKey, nodeId, 0})
    end)

    {:noreply,
     {fingerTable, predecessor, successor, mbits, nodeId, key, completedReqHopCount, noOfReq}}
  end

  defp generateRandomKey(mbits) do
    aIpAddr = [:rand.uniform(255), :rand.uniform(255), :rand.uniform(255), :rand.uniform(255)]
    hashKey = aIpAddr |> Enum.join(":")
    hashKey = :crypto.hash(:sha, hashKey) |> Base.encode16()

    hashKey =
      String.slice(hashKey, (String.length(hashKey) - div(mbits, 4))..String.length(hashKey))

    hashKey
  end

  def handle_cast(
        {:searchCompleted, newSuccessor, hopCount},
        {fingerTable, predecessor, successor, mbits, nodeId, key, completedReqHopCount, noOfReq}
      ) do
    completedReqHopCount = completedReqHopCount ++ [hopCount]

    if(length(completedReqHopCount) == noOfReq) do
      avgHop = Enum.sum(completedReqHopCount) / noOfReq
      Logger.info("Node : #{inspect(nodeId)}  hopCount: #{inspect(avgHop)} ")
      GenServer.cast(:genMain, {:completedReq, nodeId, avgHop})
    end

    {:noreply,
     {fingerTable, predecessor, successor, mbits, nodeId, key, completedReqHopCount, noOfReq}}
  end
end
