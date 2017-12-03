package dbcreate;

import javax.swing.*;
import java.awt.event.*;

public class DriverWindow extends JFrame{
	private static final long serialVersionUID = -6715331547245564085L;
	private JButton btnCountLetters = new JButton("Count Letters");
	private JButton btnReverseLetters = new JButton("Reverse Letters");
	private JButton btnRemoveDuplicates = new JButton("Remove Duplicates");
	private JTextField txtInput = new JTextField();
	private JTextField txtOutput = new JTextField();
	
	public DriverWindow()
	{	// Initialize all components
		setTitle("String Utility");
		setBounds(200, 120, 500, 620);
		//setResizable(false);
		setDefaultCloseOperation(EXIT_ON_CLOSE);
		setLayout(null);
		btnCountLetters.setBounds(25, 10, 150, 20);
		btnReverseLetters.setBounds(25, 35, 150, 20);
		btnRemoveDuplicates.setBounds(25, 60, 150, 20);
		add(btnCountLetters);
		add(btnReverseLetters);
		//add(btnRemoveDuplicates);
		txtInput.setBounds(200, 10, 175, 20);
		txtOutput.setBounds(200, 35, 175, 20);
		txtOutput.setEnabled(false);
		txtOutput.setFocusable(false);
		add(txtInput);
		add(txtOutput);
		
		btnCountLetters.addActionListener(new ActionListener()
		{
			@Override
			public void actionPerformed(ActionEvent e)
			{
				int count = 4;
				txtOutput.setText(Integer.toString(count));
			}
		});
		btnReverseLetters.addActionListener(new ActionListener()
		{
			@Override
			public void actionPerformed(ActionEvent e)
			{
				String strWord = "reverse"; //LetterUtilities.reverseLetters(txtInput.getText());
				txtOutput.setText(strWord);
			}
		});
		btnRemoveDuplicates.addActionListener(new ActionListener()
		{
			@Override
			public void actionPerformed(ActionEvent e)
			{
				String strWord = "Remove duplicate";
				txtOutput.setText(strWord);
			}
		});
		setVisible(true);
	}
	
	public static void main(String args[])
	{
		new DriverWindow();
	}
}
