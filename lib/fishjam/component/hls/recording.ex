defmodule Fishjam.Component.HLS.Recording do
  @moduledoc false

  alias Fishjam.Component.HLS.EtsHelper
  alias Fishjam.Room.ID
  alias Fishjam.Utils.PathValidation

  @recordings_folder "recordings"

  @spec validate_recording(ID.id()) :: :ok | {:error, :not_found} | {:error, :invalid_recording}
  def validate_recording(id) do
    path = directory(id)

    cond do
      not PathValidation.inside_directory?(path, root_directory()) -> {:error, :invalid_recording}
      exists?(id) -> :ok
      true -> {:error, :not_found}
    end
  end

  @spec list_all() :: {:ok, [ID.id()]} | :error
  def list_all() do
    case File.ls(root_directory()) do
      {:ok, files} -> {:ok, Enum.filter(files, &exists?(&1))}
      {:error, :enoent} -> {:ok, []}
      {:error, _reason} -> :error
    end
  end

  @spec delete(ID.id()) :: :ok | {:error, :not_found} | {:error, :invalid_recording}
  def delete(id) do
    with :ok <- validate_recording(id) do
      do_delete(id)
    end
  end

  @spec directory(ID.id()) :: String.t()
  def directory(id) do
    root_directory() |> Path.join(id) |> Path.expand()
  end

  defp exists?(id) do
    path = directory(id)
    File.exists?(path) and not live_stream?(id)
  end

  defp root_directory() do
    :fishjam
    |> Application.fetch_env!(:media_files_path)
    |> Path.join(@recordings_folder)
    |> Path.expand()
  end

  defp live_stream?(id) do
    case EtsHelper.get_hls_folder_path(id) do
      {:ok, _path} -> true
      {:error, :room_not_found} -> false
    end
  end

  defp do_delete(id) do
    id
    |> directory()
    |> File.rm_rf!()

    :ok
  end
end
