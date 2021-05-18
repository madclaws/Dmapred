defmodule Dmapred.Task do
  @type work_status :: :idle | :in_progress | :completed
  @type work_type :: :map | :reduce

  @type t :: %Dmapred.Task{
          type: work_type(),
          status: work_status(),
          worker: atom() | nil,
          input: String.t() | nil
        }

  defstruct(
    type: :map,
    status: :idle,
    worker: nil,
    input: nil
  )



  @spec new(work_type(), work_status(), String.t()) :: Dmapred.Task.t()
  def new(type, status, input) do
    %Dmapred.Task{
      type: type,
      status: status,
      input: input
    }
  end
end
