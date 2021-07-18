defmodule Dmapred.Task do
  @moduledoc """
  Module for defining the task struct and its utilities
  """
  @type work_status :: :idle | :in_progress | :completed
  @type work_type :: :map | :reduce

  @type t :: %Dmapred.Task{
          id: number(),
          type: work_type(),
          status: work_status(),
          worker: atom(),
          input: String.t() | nil,
          app: atom()
        }

  defstruct(
    id: nil,
    type: :map,
    status: :idle,
    worker: nil,
    input: nil,
    app: nil
  )

  @spec new(number(), work_type(), work_status(), String.t(), atom(), atom()) :: Dmapred.Task.t()
  def new(id, type, status, worker_id, input, app) do
    %Dmapred.Task{
      id: id,
      type: type,
      status: status,
      input: input,
      worker: worker_id,
      app: app
    }
  end
end
