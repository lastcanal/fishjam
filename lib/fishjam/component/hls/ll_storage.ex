defmodule Fishjam.Component.HLS.LLStorage do
  @moduledoc false

  @behaviour Membrane.HTTPAdaptiveStream.Storage

  alias Fishjam.Component.HLS.{EtsHelper, Utils}
  alias Fishjam.Component.HLS.Local.RequestHandler
  alias Fishjam.Room.ID

  @enforce_keys [:directory, :room_id]
  defstruct @enforce_keys ++
              [sequences: %{}, partials_in_ets: %{}, table: nil]

  @type partial_ets_key :: String.t()
  @type sequence_number :: non_neg_integer()
  @type partial_in_ets ::
          {{segment_sn :: sequence_number(), partial_sn :: sequence_number()}, partial_ets_key()}
  @type manifest_name :: String.t()

  @type t :: %__MODULE__{
          directory: Path.t(),
          room_id: ID.id(),
          table: :ets.table() | nil,
          sequences: %{ manifest_name() => { sequence_number(), sequence_number() }},
          partials_in_ets: %{ manifest_name() => [partial_in_ets()] },
        }

  @ets_cached_duration_in_segments 4
  @delta_manifest_suffix "_delta.m3u8"

  @impl true
  def init(%__MODULE__{directory: directory, room_id: room_id}) do
    with {:ok, table} <- EtsHelper.add_room(room_id) do
      %__MODULE__{room_id: room_id, table: table, directory: directory}
    else
      {:error, :already_exists} ->
        raise("Can't create ets table - another table already exists for room #{room_id}")
    end
  end

  @impl true
  def store(_parent_id, name, content, metadata, context, state) do
    case context do
      %{mode: :binary, type: :segment} ->
        {:ok, state}

      %{mode: :binary, type: :partial_segment} ->
        store_partial_segment(name, content, metadata, state)

      %{mode: :binary, type: :header} ->
        store_header(name, content, state)

      %{mode: :text, type: :manifest} ->
        store_manifest(name, content, state)
    end
  end

  @impl true
  def remove(_parent_id, name, _ctx, %__MODULE__{directory: directory} = state) do
    result =
      directory
      |> Path.join(name)
      |> File.rm()

    {result, state}
  end

  defp store_partial_segment(
         segment_name,
         content,
         %{sequence_number: sequence_number, partial_name: partial_name},
         %__MODULE__{directory: directory} = state
       ) do
    result = write_to_file(directory, segment_name, content, [:binary, :append])
    {:ok, manifest_name} = Utils.get_manifest_name(segment_name)

    state =
      state
      |> update_sequence_numbers(sequence_number, manifest_name)
      |> add_partial_to_ets(partial_name, content, manifest_name)

    {result, state}
  end

  defp store_header(
         filename,
         content,
         %__MODULE__{directory: directory} = state
       ) do
    result = write_to_file(directory, filename, content, [:binary])
    {result, state}
  end

  defp store_manifest(
         filename,
         content,
         %__MODULE__{directory: directory} = state
       ) do
    result = write_to_file(directory, filename, content)

    unless filename == "index.m3u8" do
      add_manifest_to_ets(filename, content, state)
      send_update(filename, state)
    end

    {result, state}
  end

  defp add_manifest_to_ets(filename, manifest, %{table: table}) do
    if String.ends_with?(filename, @delta_manifest_suffix) do
      EtsHelper.update_delta_manifest(table, manifest, filename)
    else
      EtsHelper.update_manifest(table, manifest, filename)
    end
  end

  defp add_partial_to_ets(
         %{
           table: table,
           partials_in_ets: partials_in_ets,
           sequences: sequences,
         } = state,
         partial_name,
         content,
         manifest_name
       ) do
    EtsHelper.add_partial(table, content, partial_name)

    if partial = sequences[manifest_name] do
      partials = Map.get(partials_in_ets, manifest_name, [])
      partials_in_ets = Map.put(partials_in_ets, manifest_name, [{partial, partial_name} | partials])
      %{state | partials_in_ets: partials_in_ets}
    else
      state
    end
  end

  defp remove_partials_from_ets(
         %{
           table: table,
           partials_in_ets: partials_in_ets,
           sequences: sequences
         } = state,
         manifest_name
       ) do
    if { curr_segment_sn, _} = Map.get(sequences, manifest_name) do
      {partials, partial_to_be_removed} =
        Enum.split_with(partials_in_ets[manifest_name], fn {{segment_sn, _partial_sn}, _partial_name} ->
          segment_sn + (@ets_cached_duration_in_segments) > curr_segment_sn
        end)

      Enum.each(partial_to_be_removed, fn {_sn, partial_name} ->
        EtsHelper.delete_partial(table, partial_name)
      end)

      partials_in_ets = Map.put(partials_in_ets, manifest_name, partials)
      %{state | partials_in_ets: partials_in_ets}
    else
      state
    end
  end

  defp send_update(filename, %{
         room_id: room_id,
         table: table,
         sequences: sequences
       }) do
    manifest_name = String.replace(filename, @delta_manifest_suffix, ".m3u8")
    {segment_sn, partial_sn} = Map.get(sequences, manifest_name, {0, 0})
    if String.ends_with?(filename, @delta_manifest_suffix) do
      EtsHelper.update_delta_recent_partial(table, {segment_sn, partial_sn}, filename)
      RequestHandler.update_delta_recent_partial(room_id, {segment_sn, partial_sn}, filename)
    else
      EtsHelper.update_recent_partial(table, {segment_sn, partial_sn}, filename)
      RequestHandler.update_recent_partial(room_id, {segment_sn, partial_sn}, filename)
    end
  end

  defp update_sequence_numbers(
         %{sequences: sequences} = state,
         new_partial_sn,
         manifest_name
       ) do
    {segment_sn, partial_sn} = Map.get(sequences, manifest_name, {0, 0})
    new_segment? = new_partial_sn < partial_sn
    sequence = if new_segment? do
      { segment_sn + 1, new_partial_sn }
    else
      { segment_sn, new_partial_sn }
    end
    state = sequences
      |> Map.put(manifest_name, sequence)
      |> then(&Map.put(state, :sequences, &1))
      # If there is a new segment we want to remove partials that are too old from ets
    if new_segment? do
      remove_partials_from_ets(state, manifest_name)
    else
      state
    end
  end

  defp write_to_file(directory, filename, content, write_options \\ []) do
    directory
    |> Path.join(filename)
    |> File.write(content, write_options)
  end
end
