// Libraries
import React, {FC} from 'react'

// Components
import {Context, IconFont} from 'src/clockface'
import {ComponentColor} from '@influxdata/clockface'

interface Props {
  onDelete: () => void
  onClone: () => void
  onView: () => void
}

const EndpointCardContext: FC<Props> = ({onDelete, onClone, onView}) => {
  return (
    <Context>
      <Context.Menu icon={IconFont.EyeOpen}>
        <Context.Item
          label="View History"
          action={onView}
          testID="endpoint-card-edit"
        />
      </Context.Menu>
      <Context.Menu icon={IconFont.Duplicate} color={ComponentColor.Secondary}>
        <Context.Item label="Clone" action={onClone} />
      </Context.Menu>
      <Context.Menu
        icon={IconFont.Trash}
        color={ComponentColor.Danger}
        testID="context-delete-menu"
      >
        <Context.Item
          label="Delete"
          action={onDelete}
          testID="context-delete-task"
        />
      </Context.Menu>
    </Context>
  )
}

export default EndpointCardContext
