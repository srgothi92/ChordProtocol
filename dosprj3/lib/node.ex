defmodule CHORD.NodeChord do
  use GenServer
  require Logger

  @doc """
  Starts the GenServer.
  """
  def start_link(inputs) do
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
    nodeId = :crypto.hash(:sha, elem(inputs, 0))
    key = nodeId
    fingerTable = initizlizeFingerTable(nodeId, mbits, successor)
    {fingerTable, predecessor, successor, mbits, nodeId, key}
  end

  def initizlizeFingerTable(nodeId, mbits, successor) do
    Enum.reduce(0..(mbits - 1), %{}, fn index, fingerTable ->
      nodeIdAtIndex = rem(nodeId + :math.pow(2, index), :math.pow(2, mbits))
      if(nodeIdAtIndex <= successor) do
        Map.put(fingerTable, index, successor)
      end
    end)
  end

  def handle_cast({:join, knownNode}, {fingerTable, predecessor, _, mbits, nodeId, key}) do
    successor = GenServer.call(knownNode, {:findSuccessor, nodeId})
    GenServer.call(self(), {:notify, successor})
    {:noreply, {fingerTable, predecessor, successor, mbits, nodeId, key}}
  end

  def handle_cast({:notify, newPredecessor}, {fingerTable, _, successor, mbits, nodeId, key}) do
    {:noreply, {fingerTable, newPredecessor, successor, mbits, nodeId, key}}
  end

  def handle_call(
        {:findSuccessor, nodeIdToFind},
        {fingerTable, predecessor, successor, mbits, nodeId, key}
      ) do
    foundSuccessor =
      if nodeIdToFind > nodeId && nodeIdToFind < successor do
        successor
      else
        nearesetNode = GenServer.call(self(), {:closestPrecedingNode, nodeIdToFind})
        GenServer.call(self(), {:findSuccessor, nearesetNode})
      end

    {:reply, foundSuccessor, {fingerTable, predecessor, successor, mbits, nodeId, key}}
  end

  def handle_call(
        {:closestPrecedingNode, nodeIdToFind},
        {fingerTable, predecessor, successor, mbits, nodeId, key}
      ) do
    foundNodeId = Enum.reduce_while(mbits..1, 0,fn mapKey, _ ->
        if (Map.has_key?(fingerTable, mapKey) && nodeIdToFind > fingerTable.mapKey) do
          {:halt, fingerTable.mapKey}
        else
          {:cont, fingerTable[mbits-1]}
        end
    end)

    {:reply, foundNodeId, {fingerTable, predecessor, successor, mbits, nodeId, key}}
  end
end
