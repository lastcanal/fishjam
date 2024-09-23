defmodule Fishjam.Component.HLS.Utils do
  @moduledoc """
  A helper module for HLS component.
  """

  @segment_suffix_regex ~r/(_\d*_part)?\.m4s$/

  @doc """
  Tries to extract the manifest name from segment or partial segment name

  ## Params:

  - `segment_name`: A HLS segment name

  ## Example

    iex> Fishjam.Component.HLS.Utils.get_manifest_name("muxed_segment_32_my_manifest_5_part.m4s")
    {:ok, "my_manifest.m3u8"}

    iex> Fishjam.Component.HLS.Utils.get_manifest_name("some_thing_else")
    {:error, :unknown_segment_name_format}

  """
  # Filename example: muxed_segment_32_g2QABXZpZGVv_5_part.m4s
  def get_manifest_name(segment_name) do
    segment_name
    |> String.replace(@segment_suffix_regex, "")
    |> String.split("_")
    |> Enum.drop_while(fn(s) -> !match?({_integer, ""}, Integer.parse(s)) end)
    |> Enum.drop(1)
    |> Enum.join("_")
    |> then(fn
      ("") -> {:error, :unknown_segment_name_format}
      (name) -> {:ok, "#{name}.m3u8"}
    end)
  end

end
